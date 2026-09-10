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

#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::common::backend_topology::{
    BackendTopologyPort, BackendTopologySnapshot, BackendTopologyValidationError, LiveBackendTarget,
};
use crate::native::fragment_transport::{
    FinalTaskInfoRead, FragmentDispatcher, NativeTaskResultTransport, RootResultOutcome,
    TaskReadGrace, TaskResultTransport,
};
use crate::query_execution::artifact::{
    RuntimeFilterDeploymentReadyDistributedQuery, ValidatedNativeSubmission,
};
use crate::query_execution::completion::{PreReadyRetryBoundary, QueryAttemptReservation};
use crate::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
    DistributedQueryIntent, DistributedQueryOutcome, DistributedQueryRequest,
    PreReadyTopologyOutcome, ProfileTerminalBuilder,
};
use crate::query_execution::lifecycle_plan::{QueryCredentialLeases, QueryInitOptions};
#[cfg(test)]
use crate::query_execution::split_assignment::DEFAULT_INITIAL_DYNAMIC_FILTER_WAIT_CAP;
use crate::query_execution::split_assignment::TaskUpdateTransport;
use crate::runtime::statement_result::StatementResult;
use crate::task_execution::sources::AttemptEstablishFacts;
use novarocks_proto_codec::lifecycle::QueryOptions as ProtocolQueryOptions;
use novarocks_types::identity::{BackendProcessId, FrontendProcessId, TaskId};
use novarocks_types::{
    AttemptId, LocalQuerySequence, NativeCompatibilityId, QueryExecutionId, QueryId,
    QueryIdAttribution, QueryProcessNamespace,
};

use super::attempt_initialization::{AttemptInitializing, RoundCredentialLeaseSource};
use super::query_registry::{
    FrontendQueryRegistry, QueryFailureCause, QueryLifecycleConvergenceReader,
    QueryLifecycleConvergenceSnapshot, RuntimeFilterTerminalRollupSnapshot,
    RuntimeFilterTerminalRollupUnavailable,
};
use super::scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};
use super::split_assignment_round::{RoundSplitAssignmentPlan, SplitAssignmentRoundGuard};
use super::task_round::{
    AssembledRound, AttemptPumps, AttemptTransport, assemble_round, install_attempt_pumps,
};
use crate::metrics::{
    FrontendProcessQueryCountersSnapshot, observe_pre_ready_replan, observe_waiting_for_backend,
    record_pre_ready_effect_gate, record_pre_ready_replan,
};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::fragment_encoder::instance::encode_query_options;
use crate::native::fragment_encoder::submission::encode_native_submission;
use crate::native::task_transport::AttemptWireFacts;
use crate::native::transport::new_fragment_dispatcher;
use crate::query_execution::runtime_filter_terminal_rollup::rollup_from_release_contributions;
use crate::runtime_filter::compiler::{
    FrontendRuntimeFilterDeploymentCompilerConfig, compile_scheduled_runtime_filter_deployment,
};
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
use crate::runtime_filter::plan_encoder::encode_binding_attachment;
use crate::task_execution::completion::{WriteCompletionTracker, WriteVerdict, accept_final_info};
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::execution::ReleasedRuntimeFilterContributions;
use crate::task_execution::feedback_pump::TaskDynamicFilterReads;
use crate::task_execution::graph::TaskNode;
use crate::task_execution::remote_task::RemoteTaskState;
use crate::task_execution::round::{TaskRound, TurnReport};
use crate::task_execution::split_transport::SplitDeliveryBridge;
use crate::task_execution::status_intake::{CondvarWake, StatusIntakeWake};
use novarocks_execution::task_execution::{
    AbortCause, FinalTaskInfo, MaxWait, OperationKind, ResultByteLimit, ResultPacketSequence,
    TaskIdentity, TaskState, TerminationDetail,
};

#[cfg(test)]
const TEST_RESULT_FETCH_BYTE_LIMIT: u64 = 16 * 1024 * 1024;

trait QueryIdSource: Send + Sync + 'static {
    fn next_query_id(&self) -> Result<QueryId, DistributedQueryError>;
}

struct UniqueQueryIdSource {
    namespace: QueryProcessNamespace,
    last_issued_sequence: AtomicU64,
}

impl Default for UniqueQueryIdSource {
    fn default() -> Self {
        let (namespace, _) = uuid::Uuid::new_v4().as_u64_pair();
        Self::new(QueryProcessNamespace::new(namespace))
    }
}

impl UniqueQueryIdSource {
    fn new(namespace: QueryProcessNamespace) -> Self {
        Self {
            namespace,
            last_issued_sequence: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn with_last_issued_sequence(namespace: QueryProcessNamespace, last_issued: u64) -> Self {
        Self {
            namespace,
            last_issued_sequence: AtomicU64::new(last_issued),
        }
    }

    fn namespace(&self) -> QueryProcessNamespace {
        self.namespace
    }
}

impl QueryIdSource for UniqueQueryIdSource {
    fn next_query_id(&self) -> Result<QueryId, DistributedQueryError> {
        let last_issued = self
            .last_issued_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next <= i64::MAX as u64)
            })
            .map_err(|_| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::Failed,
                    "frontend query id local sequence is exhausted",
                )
            })?;
        let sequence = LocalQuerySequence::new(
            last_issued
                .checked_add(1)
                .expect("successful query id allocation increments the sequence"),
        )
        .expect("successful query id allocation produces a nonzero sequence");
        Ok(QueryIdAttribution::new(self.namespace, sequence).into_query_id())
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Shared coordinator test fixture provides a deterministic query id source."
)]
struct FixedQueryIdSource(QueryId);

#[cfg(test)]
impl QueryIdSource for FixedQueryIdSource {
    fn next_query_id(&self) -> Result<QueryId, DistributedQueryError> {
        Ok(self.0)
    }
}

#[allow(
    dead_code,
    reason = "Retained as the coordinator-owned live topology test fixture."
)]
pub(crate) struct FrontendLiveBackendTopology {
    state: Mutex<FrontendLiveBackendTopologyState>,
}

#[allow(
    dead_code,
    reason = "State is retained solely by the coordinator-owned live topology test fixture."
)]
struct FrontendLiveBackendTopologyState {
    revision: u64,
    live: Vec<LiveBackendTarget>,
}

#[allow(
    dead_code,
    reason = "Retained as the coordinator-owned live topology test fixture API."
)]
impl FrontendLiveBackendTopology {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(FrontendLiveBackendTopologyState {
                revision: 0,
                live: Vec::new(),
            }),
        }
    }

    fn snapshot(&self) -> Vec<LiveBackendTarget> {
        self.state
            .lock()
            .expect("frontend live backend topology lock")
            .live
            .clone()
    }

    pub(crate) fn replace(&self, revision: u64, live: Vec<LiveBackendTarget>) {
        let mut state = self
            .state
            .lock()
            .expect("frontend live backend topology lock");
        if revision >= state.revision {
            state.revision = revision;
            state.live = live;
        }
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Coordinator test fixture models fixed and sequenced backend services."
)]
enum BackendServicesSource {
    Fixed {
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
    },
    #[cfg(test)]
    Sequence {
        schedulers: Mutex<VecDeque<FrontendFragmentScheduler>>,
        dispatcher: Arc<dyn FragmentDispatcher>,
    },
}

struct QueryBackendServices {
    scheduler: FrontendFragmentScheduler,
    dispatcher: Arc<dyn FragmentDispatcher>,
    live_backends: Vec<LiveBackendTarget>,
}

#[cfg(test)]
impl BackendServicesSource {
    fn resolve(
        &self,
        topology: &[LiveBackendTarget],
    ) -> Result<QueryBackendServices, DistributedQueryError> {
        match self {
            Self::Fixed {
                scheduler,
                dispatcher,
            } => Ok(QueryBackendServices {
                scheduler: scheduler.clone(),
                dispatcher: Arc::clone(dispatcher),
                live_backends: topology.to_vec(),
            }),
            #[cfg(test)]
            Self::Sequence {
                schedulers,
                dispatcher,
            } => {
                let scheduler = schedulers
                    .lock()
                    .expect("frontend test backend sequence lock")
                    .pop_front()
                    .expect("frontend test backend sequence exhausted");
                Ok(QueryBackendServices {
                    scheduler,
                    dispatcher: Arc::clone(dispatcher),
                    live_backends: topology.to_vec(),
                })
            }
        }
    }
}

fn production_backend_services(
    topology: &[LiveBackendTarget],
    data_runtime: FrontendDataRuntime,
) -> Result<QueryBackendServices, DistributedQueryError> {
    let entries = topology
        .iter()
        .map(|target| {
            target
                .endpoint()
                .map(|endpoint| (target.backend_idx(), endpoint))
                .map_err(|error| failed(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot = FrontendBackendSnapshot::from_live_targets(topology.to_vec())?;
    Ok(QueryBackendServices {
        scheduler: FrontendFragmentScheduler::new(snapshot),
        dispatcher: new_fragment_dispatcher(&entries, data_runtime.clone()).map_err(failed)?,
        live_backends: topology.to_vec(),
    })
}

pub struct FrontendDistributedQueryCoordinator {
    backend_topology: crate::common::backend_topology::BackendTopologyService,
    #[cfg(test)]
    backend_services: Option<BackendServicesSource>,
    runtime_filter_worker_count: NonZeroUsize,
    query_ids: Arc<dyn QueryIdSource>,
    registry: Arc<FrontendQueryRegistry>,
    data_runtime: FrontendDataRuntime,
    /// Every bound the task protocol runs one attempt with, frozen at startup.
    ///
    /// Held rather than read per attempt so a deployment's bounds cannot change
    /// while the process runs.
    coordination_budgets: novarocks_query_application::coordination::CoordinationBudgets,
    transport_budget: novarocks_task_codec::TransportBudget,
    result_fetch_byte_limit: ResultByteLimit,
    /// This frontend process's own identity, minted once per process.
    ///
    /// It is half of every query context reference, so a backend can tell one
    /// frontend's contexts from a restarted frontend's. Minting it per process
    /// rather than per query is what makes that distinction meaningful.
    frontend_process_id: FrontendProcessId,
    task_update_retry_policy: crate::query_execution::split_assignment::TaskUpdateRetryPolicy,
    connector_split_initial_dynamic_filter_wait_cap: Duration,
    native_compatibility_id: NativeCompatibilityId,
}

impl FrontendDistributedQueryCoordinator {
    #[expect(
        private_interfaces,
        reason = "The public composition entrypoint receives the frontend-owned native runtime."
    )]
    pub fn new(
        runtime_filter_worker_count: NonZeroUsize,
        native_compatibility_id: NativeCompatibilityId,
        task_update_retry_policy: crate::query_execution::split_assignment::TaskUpdateRetryPolicy,
        connector_split_initial_dynamic_filter_wait_cap: Duration,
        coordination_budgets: novarocks_query_application::coordination::CoordinationBudgets,
        transport_budget: novarocks_task_codec::TransportBudget,
        result_fetch_byte_limit: ResultByteLimit,
        backend_topology: crate::common::backend_topology::BackendTopologyService,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, DistributedQueryError> {
        let query_id_source = UniqueQueryIdSource::default();
        let query_namespace = query_id_source.namespace();
        tracing::info!(
            query_process_namespace = %query_namespace,
            "frontend query process namespace initialized"
        );
        if cfg!(debug_assertions)
            && std::env::var_os(novarocks_failpoint::QUERY_LIFECYCLE_FAULT_DIR_ENV).is_some()
        {
            eprintln!(
                "NOVAROCKS_QUERY_PROCESS_NAMESPACE query_process_namespace={query_namespace}"
            );
        }
        Ok(Self {
            backend_topology,
            #[cfg(test)]
            backend_services: None,
            runtime_filter_worker_count,
            query_ids: Arc::new(query_id_source),
            registry: Arc::new(FrontendQueryRegistry::new(query_namespace)),
            data_runtime,
            coordination_budgets,
            transport_budget,
            result_fetch_byte_limit,
            frontend_process_id: FrontendProcessId::new_v7(),
            task_update_retry_policy,
            connector_split_initial_dynamic_filter_wait_cap,
            native_compatibility_id,
        })
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Coordinator test constructor retains explicit dependency injection for lifecycle coverage."
    )]
    pub(crate) fn new_for_test(
        query_id: QueryId,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        let topology = crate::topology::ClusterBackendService::from_captured_targets_for_test(
            &scheduler.live_targets(),
        );
        Self::new_for_test_with_topology(
            query_id,
            scheduler,
            dispatcher,
            runtime_filter_worker_count,
            _test_fixture,
            Arc::new(topology),
        )
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Coordinator test constructor retains an injected topology for lifecycle coverage."
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "The test constructor keeps independent topology and lifecycle fixtures explicit."
    )]
    pub(crate) fn new_for_test_with_topology(
        query_id: QueryId,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        backend_topology: crate::common::backend_topology::BackendTopologyService,
    ) -> Self {
        Self {
            coordination_budgets:
                novarocks_query_application::coordination::CoordinationBudgets::DEFAULT,
            transport_budget: novarocks_task_codec::TransportBudget::DEFAULT,
            result_fetch_byte_limit: ResultByteLimit::new(TEST_RESULT_FETCH_BYTE_LIMIT)
                .expect("the test result byte limit is nonzero"),
            frontend_process_id: FrontendProcessId::new_v7(),
            backend_topology,
            backend_services: Some(BackendServicesSource::Fixed {
                scheduler,
                dispatcher,
            }),
            runtime_filter_worker_count,
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::new(QueryProcessNamespace::new(
                query_id.high() as u64,
            ))),
            data_runtime: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            task_update_retry_policy:
                crate::query_execution::split_assignment::TaskUpdateRetryPolicy::default(),
            connector_split_initial_dynamic_filter_wait_cap:
                DEFAULT_INITIAL_DYNAMIC_FILTER_WAIT_CAP,
            native_compatibility_id: NativeCompatibilityId::new([0x71; 32]),
        }
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Coordinator test constructor retains sequenced backends for retry coverage."
    )]
    pub(crate) fn new_for_test_with_backend_sequence(
        query_id: QueryId,
        schedulers: Vec<FrontendFragmentScheduler>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        let targets = schedulers
            .iter()
            .flat_map(|scheduler| scheduler.live_targets())
            .collect::<Vec<_>>();
        let topology =
            crate::topology::ClusterBackendService::from_captured_targets_for_test(&targets);
        Self::new_for_test_with_backend_sequence_and_topology(
            query_id,
            schedulers,
            dispatcher,
            runtime_filter_worker_count,
            _test_fixture,
            Arc::new(topology),
        )
    }

    #[cfg(test)]
    #[expect(
        clippy::too_many_arguments,
        reason = "The retry fixture must inject independently evolving topology and scheduler sequences."
    )]
    pub(crate) fn new_for_test_with_backend_sequence_and_topology(
        query_id: QueryId,
        schedulers: Vec<FrontendFragmentScheduler>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        backend_topology: crate::common::backend_topology::BackendTopologyService,
    ) -> Self {
        Self {
            coordination_budgets:
                novarocks_query_application::coordination::CoordinationBudgets::DEFAULT,
            transport_budget: novarocks_task_codec::TransportBudget::DEFAULT,
            result_fetch_byte_limit: ResultByteLimit::new(TEST_RESULT_FETCH_BYTE_LIMIT)
                .expect("the test result byte limit is nonzero"),
            frontend_process_id: FrontendProcessId::new_v7(),
            backend_topology,
            backend_services: Some(BackendServicesSource::Sequence {
                schedulers: Mutex::new(schedulers.into()),
                dispatcher,
            }),
            runtime_filter_worker_count,
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::new(QueryProcessNamespace::new(
                query_id.high() as u64,
            ))),
            data_runtime: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            task_update_retry_policy:
                crate::query_execution::split_assignment::TaskUpdateRetryPolicy::default(),
            connector_split_initial_dynamic_filter_wait_cap:
                DEFAULT_INITIAL_DYNAMIC_FILTER_WAIT_CAP,
            native_compatibility_id: NativeCompatibilityId::new([0x71; 32]),
        }
    }

    pub(crate) fn convergence_reader(&self) -> Arc<dyn QueryLifecycleConvergenceReader> {
        Arc::clone(&self.registry) as Arc<dyn QueryLifecycleConvergenceReader>
    }

    pub fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }

    fn execute_request(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let statement_deadline = statement_deadline_for_request(&request)?;
        let intent = request.intent();
        let query_id = self.query_ids.next_query_id()?;
        let execution_id = execution_id_for_round(query_id, 1)?;
        self.execute_round(
            query_id,
            execution_id,
            statement_deadline,
            request,
            None,
            RoundCredentialLeaseSource::Frozen(QueryCredentialLeases::empty()),
        )
        .map_err(|error| fail_closed_one_shot_topology_retry(intent, error))
    }

    fn execute_reserved_request(
        &self,
        request: DistributedQueryRequest,
        reservation: QueryAttemptReservation,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let statement_deadline = statement_deadline_for_request(&request)?;
        let intent = request.intent();
        let query_id = reservation.query_id();
        let execution_id = reservation.execution_id();
        self.execute_round(
            query_id,
            execution_id,
            statement_deadline,
            request,
            None,
            RoundCredentialLeaseSource::Reservation {
                reservation: Some(reservation),
                observed_collected: None,
            },
        )
        .map_err(|error| fail_closed_one_shot_topology_retry(intent, error))
    }

    /// Execute exactly one move-only distributed round. A future statement
    /// controller retains the logical `query_id`, advances only the attempt
    /// id, and supplies a newly planned request here after the old pre-ready
    /// attempt has been aborted. This method never patches or reuses an old
    /// round's artifacts.
    fn execute_round(
        &self,
        query_id: QueryId,
        execution_id: QueryExecutionId,
        statement_deadline: Instant,
        request: DistributedQueryRequest,
        retry_boundary: Option<&dyn PreReadyRetryBoundary>,
        credential_lease_source: RoundCredentialLeaseSource,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let parts = request.into_parts();
        let write_stack_session = parts.write_stack_session.clone();
        let intent = parts.completion.intent();
        let write_decoder = parts
            .write_root_decode_contract
            .clone()
            .map(crate::query_execution::write_result::RootWriteResultDecoder::new);
        // Statistics collection enters only with its Core-owned typed program.
        // It never falls through to client-result construction.
        if intent == DistributedQueryIntent::Statistics && parts.statistics_program.is_none() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "statistics execution requires a typed StatisticsCollectionProgram",
            ));
        }
        if (intent == DistributedQueryIntent::Write)
            != (write_stack_session.is_some() && write_decoder.is_some())
        {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "distributed write intent, session, and Root decode contract must be present together",
            ));
        }
        self.backend_topology
            .validate_snapshot(&parts.topology)
            .map_err(pre_ready_topology_validation_error)?;
        #[cfg(test)]
        let backend_services = match &self.backend_services {
            Some(services) => services.resolve(parts.topology.targets())?,
            None => {
                production_backend_services(parts.topology.targets(), self.data_runtime.clone())?
            }
        };
        #[cfg(not(test))]
        let backend_services =
            production_backend_services(parts.topology.targets(), self.data_runtime.clone())?;
        let dispatcher = Arc::clone(&backend_services.dispatcher);
        let _query = self
            .registry
            .register(query_id, intent, Arc::clone(&dispatcher))?;
        let schedule = crate::preparation_diagnostics::observe_result(
            "attempt_instantiation",
            "schedule_attempt",
            "not-applicable",
            Some(execution_id),
            || {
                backend_services
                    .scheduler
                    .schedule(parts.artifacts.scheduling_view(), execution_id)
            },
        )?;
        let scheduled_backend_ownership = backend_services
            .scheduler
            .scheduled_backend_ownership(&schedule.backend_ids())?;
        self.backend_topology
            .validate_snapshot(&parts.topology)
            .map_err(pre_ready_topology_validation_error)?;
        self.registry
            .set_scheduled_backend_ownership(query_id, &scheduled_backend_ownership)?;
        // Split sources and the lifecycle barrier share this one stable,
        // attempt-local feedback object.  It is populated from the sealed
        // deployment below, before either control readers or the pump starts.
        let feedback_state = Arc::new(
            RuntimeFilterFeedbackState::new(execution_id, Default::default())
                .expect("empty runtime filter feedback declaration is valid"),
        );
        let connector_context = crate::connector::connector_request_context_for_deadline(
            statement_deadline,
            parts.cancellation.clone(),
        )
        .map_err(failed)?;
        let connector_context =
            credential_lease_source.connector_request_context(connector_context);
        let initializing = AttemptInitializing::new(
            execution_id,
            parts.artifacts,
            schedule,
            self.task_update_retry_policy,
            Arc::clone(&feedback_state),
            self.connector_split_initial_dynamic_filter_wait_cap,
            connector_context,
            parts.cancellation.clone(),
            self.data_runtime.clone(),
            credential_lease_source,
        )?;
        // The synchronous statement worker is the remaining T12 bridge. The
        // async initializer performs no Connector I/O on that worker and adds
        // no semaphore-waiter helper task, but this bridge still waits on the
        // actor and therefore does not complete the per-query thread cut.
        let ready = self
            .data_runtime
            .block_on(initializing.initialize())
            .map_err(failed)??;
        let (
            ready_execution_id,
            artifacts,
            schedule,
            ready_feedback_state,
            split_assignment_plan,
            credential_leases,
        ) = ready.into_parts();
        if ready_execution_id != execution_id
            || !Arc::ptr_eq(&ready_feedback_state, &feedback_state)
        {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "attempt initializer returned readiness for another attempt",
            ));
        }
        self.backend_topology
            .validate_snapshot(&parts.topology)
            .map_err(pre_ready_topology_validation_error)?;
        let binding_attachment =
            encode_binding_attachment(artifacts.runtime_filter_binding_view())?;
        let scheduled = artifacts
            .attach_runtime_filter_bindings(binding_attachment)?
            .bind_schedule(schedule)?;
        let deployment = compile_scheduled_runtime_filter_deployment(
            scheduled.runtime_filter_scheduled_view()?,
            FrontendRuntimeFilterDeploymentCompilerConfig::from_query_lifecycle(
                parts.options.runtime_filter_lifecycle(),
                self.runtime_filter_worker_count.get(),
            )?,
        )?;
        let feedback_declaration = deployment.feedback_declaration().clone();
        feedback_state
            .configure(feedback_declaration.clone())
            .map_err(|error| {
                DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
            })?;
        let runtime_filter_attachment =
            scheduled.seal_runtime_filter_deployment(deployment.contributions())?;
        let runtime_filter_ready =
            scheduled.attach_runtime_filter_deployment(runtime_filter_attachment)?;
        let timeout_ms = parts
            .statistics_program
            .as_ref()
            .map(|program| {
                program
                    .policy()
                    .attempt_timeout()
                    .as_millis()
                    .max(1)
                    .min(i64::MAX as u128) as i64
            })
            .unwrap_or_else(|| parts.options.timeout_ms().max(0));
        let statistics_decoder = parts
            .statistics_program
            .as_ref()
            .map(|program| program.result_decoder());
        let remaining_budget = statement_deadline.saturating_duration_since(Instant::now());
        if remaining_budget.is_zero() {
            return Err(failed(
                "query deadline elapsed before native lifecycle initialization",
            ));
        }
        let init_options = QueryInitOptions::new(
            execution_id,
            self.native_compatibility_id,
            backend_services.live_backends.clone(),
            &parts.options,
            ProtocolQueryOptions::parse(encode_query_options(parts.options.runtime_options()))
                .map_err(|error| {
                    failed(format!(
                        "query options protocol projection is invalid: {error}"
                    ))
                })?,
        )?
        .with_credential_leases(credential_leases);
        // A write session's catalog is a materialization input like any typed
        // scan's. Nothing else contributes it: a writer node names its catalog
        // by handle, and the backend leases a catalog from its properties, so a
        // query whose only use of a catalog is writing to it would reach a
        // backend that never materialized it and could not resolve a write
        // runtime for the handle its own plan carries.
        let init_options = match write_stack_session.as_ref() {
            Some(session) => {
                let backend_catalog = session
                    .catalog_properties()
                    .backend_execution_projection()
                    .map_err(|error| {
                        failed(format!(
                            "project write catalog for backend execution: {error}"
                        ))
                    })?;
                let catalog_set =
                    novarocks_proto_codec::catalog::CatalogSet::new([backend_catalog]).map_err(
                        |error| failed(format!("write session catalog set is invalid: {error}")),
                    )?;
                init_options.with_catalog_set(catalog_set)
            }
            None => init_options,
        };
        let handoff = RoundHandoff {
            query_id,
            execution_id,
            statement_deadline,
            timeout_ms,
            intent,
            cancellation: parts.cancellation,
            completion: parts.completion,
            topology: parts.topology,
            statistics_decoder,
            write_decoder,
            write_stack_session,
            backend_services,
            retry_boundary,
            runtime_filter_ready,
            init_options,
            feedback_declaration,
            feedback_state,
            split_assignment_plan,
            scheduled_backend_ownership,
        };
        self.execute_round_on_task_protocol(handoff)
    }

    /// The production execution path: one attempt's tasks on the task protocol.
    ///
    /// It replaces `Init -> ControlReady -> Stage -> Start` with the substrate's
    /// own sequence -- establish one query context per scheduled backend,
    /// create every task, open each exchange edge when its destinations exist
    /// -- and then reads the client's rows from the root task's own result
    /// plane. Nothing here decides completion: the state machine computes the
    /// verdicts and this loop reads them.
    #[expect(
        clippy::too_many_lines,
        reason = "one attempt's assembly, drive loop and drain are one linear story; splitting them would hide the order they must happen in"
    )]
    fn execute_round_on_task_protocol(
        &self,
        handoff: RoundHandoff<'_>,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let execution_started = Instant::now();
        let RoundHandoff {
            query_id,
            execution_id,
            statement_deadline,
            timeout_ms,
            intent,
            cancellation,
            completion,
            // The captured snapshot is kept: it is what a pre-establish
            // failure is judged against.
            topology: captured_topology,
            mut statistics_decoder,
            mut write_decoder,
            write_stack_session,
            backend_services,
            retry_boundary,
            runtime_filter_ready,
            init_options,
            feedback_declaration,
            feedback_state,
            split_assignment_plan,
            scheduled_backend_ownership,
        } = handoff;

        // The catalog lease this produces is held for the whole attempt: the FE
        // control leases that resolved this query's typed reads must outlive
        // the backends that are still reading through them.
        let mut prepared = runtime_filter_ready.prepare_task_execution(init_options)?;
        let submission_attachment = {
            let view = prepared.native_submission_view()?;
            encode_native_submission(&view).map_err(failed)?
        };
        let (submissions, root_fetch, expected_output) = prepared
            .seal_task_submission(submission_attachment)?
            .into_parts();
        if !root_fetch.uses_result_buffer() {
            // The root result plane serves only the task whose sink is the
            // query's result sink, and a read completes only once this
            // frontend has consumed the end of that stream. A root that owns
            // no result buffer could satisfy neither, so it is refused here
            // rather than waited on forever.
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "task execution requires a root fragment that owns the query result sink",
            ));
        }

        let backend_process_ids = scheduled_backend_ownership
            .iter()
            .copied()
            .collect::<BTreeMap<usize, BackendProcessId>>();
        let mut backends = Vec::with_capacity(backend_process_ids.len());
        let mut admission_epochs = BTreeMap::new();
        for target in &backend_services.live_backends {
            let Some(&process_id) = backend_process_ids.get(&target.backend_idx()) else {
                // Live but not scheduled. Freezing it would let an operation
                // reach a process this attempt never placed a task on.
                continue;
            };
            let endpoint = target
                .endpoint()
                .map_err(|error| failed(error.to_string()))?;
            backends.push((process_id, endpoint));
            admission_epochs.insert(process_id, target.admission_epoch_capability());
        }
        if backends.len() != backend_process_ids.len() {
            return Err(failed(
                "a scheduled backend is absent from this attempt's live snapshot",
            ));
        }

        // Each backend establishes the role bindings its own tasks play, so the
        // map is built over the scheduled backends rather than over whatever
        // the deployment happens to name: the deployment covers every live
        // backend, and a live one this attempt did not schedule has no context
        // to establish anything on.
        //
        // A deployment with no contributions at all is a query with no runtime
        // filters, and every context establishes the empty contribution. That
        // is the neutral form of the absent contribution the old participant
        // manifest carried, not a guessed default: the sealing step admits
        // only "one per live backend" or "none", so a missing entry while
        // others exist is a real disagreement between the filter compiler and
        // the scheduler and is refused.
        let compiled_filters = prepared.runtime_filter_contributions();
        let mut runtime_filters = Vec::with_capacity(backend_process_ids.len());
        for (&backend_idx, &process_id) in &backend_process_ids {
            let contribution = match compiled_filters.get(&backend_idx) {
                Some(contribution) => contribution.clone(),
                None if compiled_filters.is_empty() => {
                    novarocks_proto_models::novarocks::RuntimeFilterContribution::default()
                }
                None => {
                    return Err(failed(format!(
                        "runtime filter deployment has no contribution for scheduled backend \
                         {backend_idx}"
                    )));
                }
            };
            runtime_filters.push((process_id, contribution));
        }
        let establish = AttemptEstablishFacts::freeze(
            prepared.catalog_set().as_proto().clone(),
            runtime_filters,
            *prepared.init_options().query_options().as_proto(),
            prepared.init_options().credential_leases(),
        )
        .map_err(|error| failed(error.to_string()))?;
        // Taken before the facts are handed to the runner: the rotation owner
        // has to start from the exact domain every establish installs, or its
        // first rotation would be a gap every context refuses.
        let initial_credential = establish.credential().clone();
        // Taken now that the establish holds its own wire copy: the attempt
        // keeps one owner of the vended material, and the connectors' planning
        // route is re-pointed at it. Held to the end of this call so neither
        // that route nor a write session's commit finds it already dropped.
        let attempt_storage = prepared.take_terminal_storage_resolver();

        // Declared from the encoded plans, before they are consumed. A write's
        // completion turns on every writer having finished, and "this query is
        // a write" does not say which of its fragments write.
        let writer_fragments = if intent == DistributedQueryIntent::Write {
            submissions
                .iter()
                .filter(|submission| submission.declares_table_writer())
                .map(ValidatedNativeSubmission::fragment_id)
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };

        // Also read off the encoded plans before they are consumed, and only
        // when a profile will actually be rendered. A task's own report names
        // per-operator plan nodes and its stage; the fragment it ran is a fact
        // of the sealed plan, and this is the last point where that plan is in
        // hand. Without it, EXPLAIN ANALYZE has no fragment key at all -- and
        // deriving one from a task's profile tree would name a plan node the
        // task's facts do not belong to.
        let fragment_root_plan_node_ids = if intent == DistributedQueryIntent::Profile {
            submissions
                .iter()
                .map(|submission| {
                    submission
                        .fragment_root_plan_node_id()
                        .map(|root_node_id| (submission.fragment_id(), root_node_id))
                        .map_err(failed)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?
        } else {
            BTreeMap::new()
        };

        let wake = Arc::new(CondvarWake::default());
        let attempt = AttemptWireFacts {
            native_compatibility_id: self.native_compatibility_id,
        };
        let AssembledRound {
            mut round,
            split_delivery,
        } = assemble_round(
            execution_id,
            self.frontend_process_id,
            prepared.scheduling_plan(),
            prepared.fragment_edges(),
            &backend_process_ids,
            &admission_epochs,
            &backends,
            submissions,
            establish,
            Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
            AttemptTransport {
                budget: self.coordination_budgets.dispatch,
                transport: self.transport_budget,
                status_subscription_error_budget: self
                    .coordination_budgets
                    .status_subscription_error_budget,
                attempt,
                data_runtime: self.data_runtime.clone(),
            },
        )
        .map_err(|error| failed(error.to_string()))?;
        let result_transport = Arc::new(
            NativeTaskResultTransport::new(
                &backends,
                self.data_runtime.clone(),
                TaskReadGrace::new(self.transport_budget.frontend_queue_residence()),
            )
            .map_err(failed)?,
        );
        let root_task = round.root_task();

        // Split enumeration is another per-attempt owner on the same serial
        // turn. Its synchronous Connector calls run under process admission,
        // while TaskUpdate submission and acknowledgement observation remain
        // outside that permit.
        let mut split_assignment = split_assignment_plan.and_then(|plan| {
            SplitAssignmentRoundGuard::install(
                &mut round,
                execution_id,
                plan,
                Arc::clone(&split_delivery) as Arc<dyn TaskUpdateTransport>,
                self.data_runtime.clone(),
                Arc::clone(&wake)
                    as Arc<dyn crate::task_execution::status_intake::StatusIntakeWake>,
            )
        });

        // The two per-attempt feedback loops. Both hang on the runner's one
        // pump seam rather than on call sites in the drive loop below: see
        // `TaskRound::turn` for why they land between the status fold and
        // submission. The runner refuses to turn until this has run, so
        // omitting it is a query failure rather than a silently missing loop.
        let credential_rotation = install_attempt_pumps(
            &mut round,
            AttemptPumps {
                execution_id,
                feedback_state: Arc::clone(&feedback_state),
                declared_feedback_channels: feedback_declaration.channels().len(),
                reads: Arc::clone(&result_transport) as Arc<dyn TaskDynamicFilterReads>,
                initial_credential: &initial_credential,
                credential_storage: attempt_storage.clone(),
            },
        );

        let mut write_completion = if intent == DistributedQueryIntent::Write {
            let writers = round
                .execution()
                .graph()
                .tasks()
                .filter(|task| writer_fragments.contains(&task.fragment_id()))
                .map(TaskNode::identity)
                .collect::<Vec<_>>();
            Some(
                WriteCompletionTracker::try_new(root_task, writers)
                    .map_err(|error| failed(error.to_string()))?,
            )
        } else {
            None
        };

        let mut final_task_info = FinalTaskInfoCollector::new(
            result_transport.as_ref(),
            intent == DistributedQueryIntent::Profile,
        );
        // The task protocol's analogue of the two lifecycle gates: every query
        // context established is ControlReady, every task created is Stage and
        // Start. Both close exactly once, and until the first one closes a
        // failure may still be a replaced backend rather than this query's.
        let mut contexts_established = false;
        let mut tasks_created = false;
        // The membership owner gets the same window the old Init RPC had to
        // prove a replacement, taken from the task protocol's own establish
        // cap rather than a second number invented here.
        let establish_wait = MaxWait::default_for(OperationKind::UpdateQueryContext).get();
        let mut batches = Vec::new();
        // Recorded rather than inferred, exactly as the old path recorded it: a
        // write commits on the strength of this fact.
        let mut observed_result_eof = false;
        let mut last_root_poll = RootResultPoll::default();
        let mut root_batch_count = 0_u64;
        let mut root_row_count = 0_u64;
        // Taken before the loop because the polls run beside it: the schema
        // is shared, immutable and only read, while `expected_output` itself
        // is consumed by this attempt's answer.
        let root_output_schema = Arc::clone(expected_output.fetch_view().chunk_schema());
        let mut root_result_polls: Option<RootResultPolls> = None;
        let mut wait_witness = TaskRoundWaitWitness::new(
            task_round_wait_facts(
                &round,
                root_task,
                0,
                last_root_poll,
                write_completion.as_mut(),
            ),
            Instant::now(),
        );
        let outcome = loop {
            // Recomputed each turn so the two gates and the classification
            // window are read from this turn's state rather than last turn's.
            let classification = TaskRoundFailureClassification {
                before_contexts_established: !contexts_established,
                captured: &captured_topology,
                observation_deadline: statement_deadline.min(
                    Instant::now()
                        .checked_add(establish_wait)
                        .unwrap_or(statement_deadline),
                ),
                cancellation: &cancellation,
            };
            if cancellation.is_cancelled() {
                break Err(self.fail_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    classification,
                    QueryFailureCause::ClientCancellation,
                    "query cancelled while fetching result",
                ));
            }
            if let Some(message) = self.registry.first_failure(query_id) {
                break Err(self.fail_latched_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    message,
                ));
            }
            let now = Instant::now();
            if now >= statement_deadline {
                // The message names the facts rather than only the elapsed
                // time. A completion rule that waits on absent facts has to
                // say which one was absent, or its timeout is indistinguishable
                // from every other timeout.
                let waiting_on = task_round_wait_facts(
                    &round,
                    root_task,
                    batches.len(),
                    last_root_poll,
                    write_completion.as_mut(),
                );
                break Err(self.fail_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    classification,
                    QueryFailureCause::FrontendExecution,
                    format!("query timed out after {timeout_ms} ms waiting on {waiting_on}"),
                ));
            }

            let mut moved = match advance_task_round(&mut round, &split_delivery) {
                Ok(report) => !report.is_idle(),
                Err(error) => {
                    break Err(self.fail_task_round(
                        query_id,
                        &mut round,
                        &split_delivery,
                        classification,
                        QueryFailureCause::FrontendExecution,
                        format!("task execution did not advance: {error}"),
                    ));
                }
            };
            final_task_info.observe(&round);
            if !contexts_established && round.contexts_established() {
                contexts_established = true;
                if let Some(retry_boundary) = retry_boundary {
                    retry_boundary.close_after_control_ready();
                }
            }
            if contexts_established && !tasks_created && round.tasks_created() {
                tasks_created = true;
                if let Some(retry_boundary) = retry_boundary {
                    retry_boundary.close_after_stage_or_start();
                }
            }
            // Re-read after the gates moved: a failure seen from here on is
            // judged against the window this turn actually reached.
            let classification = TaskRoundFailureClassification {
                before_contexts_established: !contexts_established,
                captured: &captured_topology,
                observation_deadline: statement_deadline.min(
                    Instant::now()
                        .checked_add(establish_wait)
                        .unwrap_or(statement_deadline),
                ),
                cancellation: &cancellation,
            };
            if let Some(detail) = round.failure_cause() {
                let waiting_on = task_round_wait_facts(
                    &round,
                    root_task,
                    batches.len(),
                    last_root_poll,
                    write_completion.as_mut(),
                );
                let detail =
                    format!("task execution terminated: {detail:?}; terminal_round={waiting_on}");
                break Err(self.fail_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    classification,
                    QueryFailureCause::BackendLocalFailure,
                    detail,
                ));
            }
            // A deliverer that failed will never feed the scans it had left,
            // so the round cannot reach its own exit and would otherwise run
            // out the statement deadline reporting the wait instead of the
            // cause. Read every turn, and only a failure ends the attempt:
            // a worker that stopped because every source went terminal is the
            // normal case and says nothing about the query.
            let delivery_failure = split_assignment
                .as_ref()
                .and_then(SplitAssignmentRoundGuard::failure)
                .map(|error| format!("split assignment stopped delivering: {error}"));
            if let Some(detail) = delivery_failure {
                break Err(self.fail_task_round(
                    query_id,
                    &mut round,
                    &split_delivery,
                    classification,
                    QueryFailureCause::FrontendExecution,
                    detail,
                ));
            }

            // The polls run until this loop has observed the end of the
            // result stream, and that fact alone stops them. Not the root
            // task leaving its created state: delivering the end of stream is
            // what retires that task, so stopping on its terminal status
            // races the last answer -- and losing that answer leaves the read
            // waiting for an end of stream nothing will send again.
            //
            // Its creation being acknowledged is still what starts the first
            // poll, because the result plane refuses a poll for a task it
            // does not hold yet.
            if observed_result_eof {
                root_result_polls = None;
            } else if root_result_polls.is_none() && task_is_created(&round, root_task) {
                match RootResultPolls::start(
                    Arc::clone(&result_transport) as Arc<dyn TaskResultTransport>,
                    root_task,
                    Arc::clone(&root_output_schema),
                    statement_deadline,
                    self.result_fetch_byte_limit,
                    Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
                    self.data_runtime.clone(),
                ) {
                    Ok(polls) => {
                        emit_distributed_write_phase_marker(
                            intent,
                            execution_id,
                            "root_poll_enter",
                            execution_started,
                            None,
                        );
                        root_result_polls = Some(polls);
                    }
                    Err(error) => {
                        break Err(self.fail_task_round(
                            query_id,
                            &mut round,
                            &split_delivery,
                            classification,
                            QueryFailureCause::FrontendExecution,
                            error,
                        ));
                    }
                }
            }
            // One answer per turn. Consuming it makes the turn non-idle, so
            // the loop comes straight back for the next one instead of
            // parking on the wake the poller raised.
            if let Some(answer) = root_result_polls.as_mut().and_then(RootResultPolls::take) {
                match answer {
                    Ok(RootResultOutcome::Ready {
                        packet_sequence,
                        batch,
                    }) => {
                        if let Err(error) = round.consume_root_result_packet(packet_sequence, false)
                        {
                            emit_distributed_write_phase_marker(
                                intent,
                                execution_id,
                                "root_failure",
                                execution_started,
                                Some((root_batch_count, root_row_count)),
                            );
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                QueryFailureCause::FrontendExecution,
                                format!("root result packet was refused: {error}"),
                            ));
                        }
                        root_batch_count = root_batch_count.saturating_add(1);
                        let mut batch_rows = 0;
                        if let Some(decoder) = statistics_decoder.as_mut() {
                            let chunk = batch.into_chunk();
                            batch_rows = chunk.len();
                            if let Err(error) = decoder.apply_chunk(&chunk) {
                                emit_distributed_write_phase_marker(
                                    intent,
                                    execution_id,
                                    "root_failure",
                                    execution_started,
                                    Some((root_batch_count, root_row_count)),
                                );
                                break Err(self.fail_task_round(
                                    query_id,
                                    &mut round,
                                    &split_delivery,
                                    classification,
                                    QueryFailureCause::FrontendExecution,
                                    error,
                                ));
                            }
                        } else if let Some(decoder) = write_decoder.as_mut() {
                            let chunk = batch.into_chunk();
                            batch_rows = chunk.len();
                            if let Err(error) = decoder.apply_chunk(&chunk) {
                                emit_distributed_write_phase_marker(
                                    intent,
                                    execution_id,
                                    "root_failure",
                                    execution_started,
                                    Some((root_batch_count, root_row_count)),
                                );
                                break Err(self.fail_task_round(
                                    query_id,
                                    &mut round,
                                    &split_delivery,
                                    classification,
                                    QueryFailureCause::FrontendExecution,
                                    error,
                                ));
                            }
                        } else {
                            batches.push(batch);
                        }
                        root_row_count = root_row_count
                            .saturating_add(u64::try_from(batch_rows).unwrap_or(u64::MAX));
                        if root_batch_count == 1 {
                            emit_distributed_write_phase_marker(
                                intent,
                                execution_id,
                                "root_first_packet",
                                execution_started,
                                Some((root_batch_count, root_row_count)),
                            );
                        }
                        if let Err(error) = root_result_polls
                            .as_ref()
                            .expect("a root result answer requires its poll owner")
                            .acknowledge(packet_sequence)
                        {
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                QueryFailureCause::FrontendExecution,
                                error,
                            ));
                        }
                        last_root_poll = RootResultPoll::Packet(packet_sequence);
                        moved = true;
                    }
                    Ok(RootResultOutcome::EndOfStreamPending { packet_sequence }) => {
                        if let Err(error) = root_result_polls
                            .as_ref()
                            .expect("a root result answer requires its poll owner")
                            .acknowledge(packet_sequence)
                        {
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                QueryFailureCause::FrontendExecution,
                                error,
                            ));
                        }
                        last_root_poll = RootResultPoll::Packet(packet_sequence);
                        moved = true;
                    }
                    Ok(RootResultOutcome::EndOfStream { packet_sequence }) => {
                        if let Err(error) = round.consume_root_result_packet(packet_sequence, true)
                        {
                            emit_distributed_write_phase_marker(
                                intent,
                                execution_id,
                                "root_failure",
                                execution_started,
                                Some((root_batch_count, root_row_count)),
                            );
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                QueryFailureCause::FrontendExecution,
                                format!("root result end of stream was refused: {error}"),
                            ));
                        }
                        if let Some(decoder) = statistics_decoder.as_mut()
                            && let Err(error) = decoder.observe_root_eof()
                        {
                            emit_distributed_write_phase_marker(
                                intent,
                                execution_id,
                                "root_failure",
                                execution_started,
                                Some((root_batch_count, root_row_count)),
                            );
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                QueryFailureCause::FrontendExecution,
                                error,
                            ));
                        }
                        if let Some(decoder) = write_decoder.as_mut()
                            && let Err(error) = decoder.observe_root_eof()
                        {
                            emit_distributed_write_phase_marker(
                                intent,
                                execution_id,
                                "root_failure",
                                execution_started,
                                Some((root_batch_count, root_row_count)),
                            );
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                QueryFailureCause::FrontendExecution,
                                error,
                            ));
                        }
                        emit_distributed_write_phase_marker(
                            intent,
                            execution_id,
                            "root_eof",
                            execution_started,
                            Some((root_batch_count, root_row_count)),
                        );
                        observed_result_eof = true;
                        last_root_poll = RootResultPoll::EndOfStream(packet_sequence);
                        moved = true;
                    }
                    Ok(RootResultOutcome::NotReady) => {
                        // Deliberately not progress. The poller is already
                        // asking again, so a turn that claimed this moved
                        // something would keep the loop spinning through
                        // that whole wait instead of parking until the
                        // poller, an acknowledgement or a status event wakes
                        // it.
                        last_root_poll = RootResultPoll::NotReady;
                    }
                    Ok(RootResultOutcome::Failed(detail)) => {
                        emit_distributed_write_phase_marker(
                            intent,
                            execution_id,
                            "root_failure",
                            execution_started,
                            Some((root_batch_count, root_row_count)),
                        );
                        // A refused or failed poll is also how a read ends
                        // when the task it names has already gone. If this
                        // attempt has a failure of its own, that failure is
                        // the cause and this answer is the reaction to it, so
                        // it is the one reported.
                        let detail = round.failure_cause().map_or(detail, |cause| {
                            format!("task execution terminated: {cause:?}")
                        });
                        break Err(self.fail_task_round(
                            query_id,
                            &mut round,
                            &split_delivery,
                            classification,
                            QueryFailureCause::BackendLocalFailure,
                            detail,
                        ));
                    }
                    Err(detail) => {
                        let (cause, detail) = round.failure_cause().map_or(
                            (QueryFailureCause::RemoteTransportObservation, detail),
                            |task_failure| {
                                (
                                    QueryFailureCause::BackendLocalFailure,
                                    format!("task execution terminated: {task_failure:?}"),
                                )
                            },
                        );
                        break Err(self.fail_task_round(
                            query_id,
                            &mut round,
                            &split_delivery,
                            classification,
                            cause,
                            detail,
                        ));
                    }
                }
            }

            wait_witness.observe(
                execution_id,
                task_round_wait_facts(
                    &round,
                    root_task,
                    batches.len(),
                    last_root_poll,
                    write_completion.as_mut(),
                ),
                Instant::now(),
            );

            if round.client_visible_completion() {
                match write_completion.as_mut() {
                    // A write's completion is not the read's. Every declared
                    // writer must reach a success-compatible terminal and the
                    // root finish task must publish FINISHED. The independently
                    // decoded Root prepared set later proves that every writer
                    // output actually arrived.
                    Some(tracker) => {
                        observe_write_statuses(&round, tracker);
                        let verdict = tracker.execution_verdict(round.failure_cause().is_some());
                        if verdict.is_complete() {
                            break Ok(());
                        }
                        if !verdict.is_pending() {
                            break Err(self.fail_task_round(
                                query_id,
                                &mut round,
                                &split_delivery,
                                classification,
                                QueryFailureCause::FrontendExecution,
                                format!(
                                    "distributed write reached a non-committable terminal: {verdict}"
                                ),
                            ));
                        }
                    }
                    None if intent == DistributedQueryIntent::Statistics
                        && !statistics_tasks_are_terminal(&round) =>
                    {
                        // Root EOF proves the statistics artifact stream is
                        // complete, but an upstream stand-down can still be
                        // converging through CANCELING. Wait for the frozen
                        // task set to reach terminals before classifying those
                        // terminals as success-compatible or failed.
                    }
                    None => break Ok(()),
                }
            }
            if !moved {
                wake.wait(TASK_ROUND_IDLE_WAIT);
            }
        };
        // Nothing rotates or prunes for a query whose answer is already
        // decided, success or failure. Both loops are stopped before the
        // outcome is propagated: the drain below keeps turning, and a rotation
        // started there would be judged against a hard deadline for material
        // no task still reads -- which would turn a linearized completion into
        // a failure. Closing the feedback state also wakes any split source
        // still inside its initial wait.
        if let Some(rotation) = &credential_rotation {
            rotation.wipe();
        }
        feedback_state.close();
        outcome?;

        // The client-visible completion is linearized. Draining closes this
        // attempt's internal resources and releases every query context; it
        // must never be able to take that completion back, so a drain that
        // does not finish inside the statement's own budget is reported rather
        // than turned into a query failure.
        emit_distributed_write_phase_marker(
            intent,
            execution_id,
            "drain_enter",
            execution_started,
            None,
        );
        drain_task_round(
            &mut round,
            &split_delivery,
            &wake,
            split_assignment.as_ref(),
            statement_deadline,
            self.transport_budget.frontend_queue_residence(),
            execution_id,
            &mut final_task_info,
        );
        emit_distributed_write_phase_marker(
            intent,
            execution_id,
            "drain_exit",
            execution_started,
            None,
        );

        // Read once the drain has ended: a release acknowledgement is the only
        // message that carries a backend's sealed runtime-filter observation,
        // and the drain is what waits for every one of them. Read before the
        // intent match so the same set feeds the convergence evidence and the
        // profile -- two reads could disagree, and they are the same fact.
        let runtime_filter_contributions =
            round.execution().released_runtime_filter_contributions();
        self.publish_task_round_convergence(execution_id, &runtime_filter_contributions);

        // The split worker blocks on acknowledgements the drain above settles,
        // so joining it is safe only now that it has stopped. A worker still
        // waiting is woken with an unknown outcome, and the guard's own stop
        // then keeps it from resending.
        let split_assignment_profile = match split_assignment.take() {
            Some(assignment) => {
                emit_distributed_write_phase_marker(
                    intent,
                    execution_id,
                    "split_finish_enter",
                    execution_started,
                    None,
                );
                let abandoned = !assignment.is_finished();
                if abandoned {
                    split_delivery.abandon("split assignment round ended with the attempt");
                }
                match assignment.finish_after_attempt(abandoned) {
                    Ok(profile) => {
                        emit_distributed_write_phase_marker(
                            intent,
                            execution_id,
                            "split_finish_exit",
                            execution_started,
                            None,
                        );
                        profile
                    }
                    Err(error) => {
                        return Err(self.fail_and_cancel(
                            query_id,
                            format!("split assignment did not finish: {error}"),
                        ));
                    }
                }
            }
            None => novarocks_spi::connector::read_stack::SplitSourceProfile::default(),
        };

        if let Some(session) = write_stack_session.as_ref()
            && let Some(resolver) = attempt_storage.as_ref()
        {
            // A connector write reaches its external commit after this attempt
            // finalizes, and on a vended-credential deployment that commit
            // reads object storage through the attempt's own leases.
            session
                .retain_terminal_storage_resolver(Arc::clone(resolver)
                    as Arc<dyn novarocks_spi::connector::ConnectorStorageResolver>);
        }

        let outcome = (|| match intent {
            DistributedQueryIntent::Result => {
                completion.result(expected_output.into_query_result(batches)?)
            }
            DistributedQueryIntent::Write => {
                let session = write_stack_session.ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "distributed write execution has no connector write session",
                    )
                })?;
                let tracker = write_completion.as_mut().ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "distributed write execution has no write completion tracker",
                    )
                })?;
                let decoder = write_decoder.take().ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "distributed write execution lost its Root decoder",
                    )
                })?;

                let mut barrier = crate::query_execution::write_barrier::WriteCommitBarrier::new();
                observe_write_statuses(&round, tracker);
                let execution_verdict = tracker.execution_verdict(round.failure_cause().is_some());
                emit_distributed_write_phase_marker(
                    intent,
                    execution_id,
                    "write_decoder_finish_enter",
                    execution_started,
                    None,
                );
                let prepared_write_set = decoder.finish().map_err(|error| {
                    emit_distributed_write_phase_marker(
                        intent,
                        execution_id,
                        "root_failure",
                        execution_started,
                        Some((root_batch_count, root_row_count)),
                    );
                    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
                })?;
                emit_distributed_write_phase_marker(
                    intent,
                    execution_id,
                    "write_decoder_finish_exit",
                    execution_started,
                    Some((root_batch_count, root_row_count)),
                );
                barrier.observe_prepared_write_set(prepared_write_set);
                barrier.observe_task_execution(execution_verdict);
                if cancellation.is_cancelled() {
                    barrier.observe_cancelled();
                }
                if Instant::now() >= statement_deadline {
                    barrier.observe_deadline_expired();
                }

                let prepared_set = barrier.into_committable().map_err(|blocked| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        blocked.as_str(),
                    )
                })?;
                completion.write_session_outcome(session, prepared_set)
            }
            DistributedQueryIntent::Profile => {
                let result = expected_output.into_query_result(batches)?;
                let mut builder = ProfileTerminalBuilder::new();
                let graph = round.execution().graph();
                for info in &final_task_info.collected {
                    // Stage to fragment to sealed fragment root: each hop is a
                    // fact this attempt froze, so a task whose stage or
                    // fragment cannot be resolved is a disagreement between the
                    // graph and the plan, not a profile to publish partially.
                    let stage_id = info.final_status().identity().stage_id();
                    let fragment_id = graph
                        .stage(stage_id)
                        .ok_or_else(|| {
                            failed(format!(
                                "final task info names stage {stage_id} which this attempt's task \
                                 graph does not contain"
                            ))
                        })?
                        .fragment_id();
                    let root_node_id = fragment_root_plan_node_ids
                        .get(&fragment_id)
                        .copied()
                        .ok_or_else(|| {
                            failed(format!(
                                "stage {stage_id} names fragment {fragment_id} which this \
                                 attempt's sealed submissions do not contain"
                            ))
                        })?;
                    builder.apply_task_operator_statistics(info, root_node_id)?;
                }
                // The runtime-filter half of the profile comes from the
                // participants' own terminal observations, not from any task's
                // operator statistics: the row effects a filter applies are
                // folded by the query context's participant, and a task's
                // projection never carried them.
                for (process_id, telemetry) in runtime_filter_contributions.contributions() {
                    builder.apply_runtime_filter_contribution(
                        execution_id,
                        *process_id,
                        telemetry,
                    )?;
                }
                builder.apply_split_assignment_profile(split_assignment_profile);
                completion.profile(result, builder.finish())
            }
            DistributedQueryIntent::Statistics => {
                if let Some(error) = statistics_all_success_error(&round) {
                    return Err(error);
                }
                let mut decoder = statistics_decoder.take().ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "statistics execution lost its Root decoder",
                    )
                })?;
                decoder.observe_execution_success().map_err(|error| {
                    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
                })?;
                let artifacts = decoder.finish().map_err(|error| {
                    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
                })?;
                completion.statistics(artifacts)
            }
        })();
        if let Err(error) = &outcome {
            let _ = self.registry.latch_failure(
                query_id,
                QueryFailureCause::FrontendExecution,
                error.message().to_string(),
            );
            return Err(DistributedQueryError::new(error.kind(), error.message()));
        }
        outcome
    }

    /// Publishes this task-protocol attempt's immutable convergence evidence.
    ///
    /// Called once, after the drain, because that is the point at which every
    /// participant's contribution is either in hand or will never arrive.
    ///
    /// What this attempt genuinely does not have is not invented here. The
    /// task protocol mints no per-participant proof or attestation at all: a
    /// task's terminal is its own status, which is why the retired protocol's
    /// participant-outcome list is gone from this evidence rather than
    /// published empty. `error_source` and `primary_error` are absent for a
    /// different reason: this path is reached only after the attempt's answer
    /// is already linearized as a success, and a failed attempt aborts its
    /// contexts instead of releasing them, so it has no sealed contribution to
    /// publish at all.
    ///
    /// `metrics` carries the process-scoped frontend query counters. The task
    /// protocol has no producer for that set -- the retired lifecycle chain was
    /// its only one -- so the rollup reports the default rather than inventing
    /// a value it does not own. See
    /// [`crate::metrics::process_query_counters`] for what retiring the set
    /// would take.
    fn publish_task_round_convergence(
        &self,
        execution_id: QueryExecutionId,
        contributions: &ReleasedRuntimeFilterContributions,
    ) {
        // A context whose release never answered may still have observed
        // runtime-filter activity this frontend has not seen, so the rollup is
        // refused rather than summed over the backends that did answer.
        let runtime_filter = if contributions.is_complete() {
            RuntimeFilterTerminalRollupSnapshot::Available(rollup_from_release_contributions(
                contributions.contributions(),
            ))
        } else {
            RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            )
        };
        tracing::debug!(
            execution_id = ?execution_id,
            contributions = contributions.contributions().len(),
            contexts = contributions.contexts(),
            complete = contributions.is_complete(),
            "task protocol attempt published its runtime filter convergence evidence"
        );
        self.registry
            .publish_task_round_convergence(QueryLifecycleConvergenceSnapshot {
                execution_id,
                error_source: None,
                primary_error: None,
                runtime_filter,
                metrics: FrontendProcessQueryCountersSnapshot::default(),
            });
    }

    /// Stands every query context of this attempt down, then classifies why it
    /// failed.
    ///
    /// A dropped round would leave the backends holding tasks until their
    /// lease expired; an explicit abort is what makes a failed attempt release
    /// its resources at the moment the frontend gave up on it.
    ///
    /// A failure that arrived before every query context was established gets
    /// the same treatment the old barrier gave a pre-ControlReady one: it is
    /// marked as needing a topology observation, and the membership owner --
    /// not this failure's text -- decides whether an exact captured process
    /// was replaced. Nothing is latched in that case, because a replanned
    /// round registers its own attempt and the failure belongs to the one
    /// being abandoned.
    fn fail_task_round(
        &self,
        query_id: QueryId,
        round: &mut TaskRound,
        split_delivery: &SplitDeliveryBridge,
        classification: TaskRoundFailureClassification<'_>,
        cause: QueryFailureCause,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        let message = message.into();
        split_delivery.abandon(message.clone());
        // Released to the transport before any judgement below, and released
        // unconditionally: a backend that applies its establish after this
        // frontend has given up must not be left holding the context. What
        // the judgement decides is only whether this worker waits, never
        // whether the backends are told.
        abort_task_round(round, &message);
        match judge_pre_ready_task_round_failure(
            self.backend_topology.as_ref(),
            classification,
            &message,
        ) {
            Some(classified) => classified,
            None => self.fail_and_cancel_with_cause(query_id, cause, message),
        }
    }

    /// Stands down a task round for a failure the registry already selected.
    ///
    /// Relatching that message as a frontend execution failure would discard
    /// its typed origin and could incorrectly supersede an earlier lifecycle
    /// observation. The existing registry record remains the single causal
    /// authority; this helper only performs the task-protocol cleanup.
    fn fail_latched_task_round(
        &self,
        query_id: QueryId,
        round: &mut TaskRound,
        split_delivery: &SplitDeliveryBridge,
        message: String,
    ) -> DistributedQueryError {
        split_delivery.abandon(message.clone());
        abort_task_round(round, &message);
        failed(self.registry.first_failure(query_id).unwrap_or(message))
    }

    fn fail_and_cancel(
        &self,
        query_id: QueryId,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        self.fail_and_cancel_with_cause(query_id, QueryFailureCause::FrontendExecution, message)
    }

    fn fail_and_cancel_with_cause(
        &self,
        query_id: QueryId,
        cause: QueryFailureCause,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        match self.registry.latch_failure(query_id, cause, message) {
            Ok(failure) => failed(failure.message().to_string()),
            Err(error) => error,
        }
    }
}

impl DistributedQueryCoordinator for FrontendDistributedQueryCoordinator {
    fn reserve_logical_query(
        &self,
    ) -> Result<crate::query_execution::completion::LogicalQueryReservation, DistributedQueryError>
    {
        let query_id = self.query_ids.next_query_id()?;
        crate::preparation_diagnostics::bind_logical_query(query_id);
        Ok(crate::query_execution::completion::LogicalQueryReservation::new(query_id))
    }

    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }

    fn execute_reserved(
        &self,
        request: DistributedQueryRequest,
        reservation: QueryAttemptReservation,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_reserved_request(request, reservation)
    }

    fn execute_prepared(
        &self,
        operation: crate::query_execution::completion::PreparedDistributedQuery,
    ) -> Result<StatementResult, DistributedQueryError> {
        let (first_request, first_completion, mut attempt_factory, logical_reservation) =
            operation.into_parts();
        let query_id = logical_reservation.into_query_id();
        let first_revision = first_request.topology().revision();
        let retry_deadline = statement_deadline_for_request(&first_request)?;
        let first_retry_boundary = attempt_factory
            .as_deref()
            .map(|factory| factory as &dyn PreReadyRetryBoundary);
        let first_reservation =
            crate::query_execution::completion::QueryAttemptReservation::first(query_id)?;
        let first_execution_id = first_reservation.execution_id();
        match self.execute_round(
            query_id,
            first_execution_id,
            retry_deadline,
            first_request,
            first_retry_boundary,
            RoundCredentialLeaseSource::Reservation {
                reservation: Some(first_reservation),
                observed_collected: None,
            },
        ) {
            Ok(outcome) => first_completion.complete(outcome).map_err(failed),
            Err(first_error) => {
                // Never classify ordinary failure text as a topology retry.
                // The lifecycle barrier and pre-Init validation are the sole
                // typed evidence sources.
                if first_error.pre_ready_topology_outcome().is_none() {
                    return Err(first_error);
                }
                let Some(factory) = attempt_factory.as_deref_mut() else {
                    return Err(first_error);
                };
                let reason = pre_ready_topology_reason(
                    first_error
                        .pre_ready_topology_outcome()
                        .expect("pre-ready topology outcome checked above"),
                );
                if let Err(error) = factory.permit_pre_ready_retry() {
                    record_pre_ready_effect_gate("rejected");
                    return Err(error);
                }
                record_pre_ready_effect_gate("permitted");
                record_pre_ready_replan(reason);
                let waiting_started_at = Instant::now();
                let fresh_topology = self
                    .backend_topology
                    .wait_for_eligible_after(first_revision, retry_deadline)
                    .map_err(|error| {
                        observe_waiting_for_backend(waiting_started_at.elapsed());
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::Failed,
                            error.to_string(),
                        )
                    })?;
                observe_waiting_for_backend(waiting_started_at.elapsed());
                let instantiation_started_at = Instant::now();
                let replacement = factory.instantiate(fresh_topology);
                observe_pre_ready_replan(instantiation_started_at.elapsed());
                let replacement = replacement?;
                let (replacement_request, replacement_completion) = replacement.into_parts();
                let replacement_reservation =
                    crate::query_execution::completion::QueryAttemptReservation::retry(
                        query_id, 2,
                    )?;
                let replacement_execution_id = replacement_reservation.execution_id();
                self.execute_round(
                    query_id,
                    replacement_execution_id,
                    retry_deadline,
                    replacement_request,
                    None,
                    RoundCredentialLeaseSource::Reservation {
                        reservation: Some(replacement_reservation),
                        observed_collected: None,
                    },
                )
                .and_then(|outcome| replacement_completion.complete(outcome).map_err(failed))
            }
        }
    }

    fn execute_prepared_raw(
        &self,
        operation: crate::query_execution::completion::PreparedRetriableDistributedRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let (first_request, mut round_factory, reservation) = operation.into_parts();
        let reservation = match reservation {
            Some(reservation) => reservation,
            None => crate::query_execution::completion::QueryAttemptReservation::first(
                self.query_ids.next_query_id()?,
            )?,
        };
        let query_id = reservation.query_id();
        let first_execution_id = reservation.execution_id();
        let retry_collected_vended_credentials = Arc::new(AtomicBool::new(
            reservation.has_collected_credential_leases(),
        ));
        let first_revision = first_request.topology().revision();
        let retry_deadline = statement_deadline_for_request(&first_request)?;
        match self.execute_round(
            query_id,
            first_execution_id,
            retry_deadline,
            first_request,
            Some(round_factory.as_ref() as &dyn PreReadyRetryBoundary),
            RoundCredentialLeaseSource::Reservation {
                reservation: Some(reservation),
                observed_collected: Some(Arc::clone(&retry_collected_vended_credentials)),
            },
        ) {
            Ok(outcome) => Ok(outcome),
            Err(first_error) => {
                if first_error.pre_ready_topology_outcome().is_none() {
                    return Err(first_error);
                }
                if retry_collected_vended_credentials.load(Ordering::Acquire) {
                    return Err(DistributedQueryError::topology_retry_unsupported(
                        first_error
                            .pre_ready_topology_outcome()
                            .expect("pre-ready topology outcome checked above"),
                        "distributed write collected vended credentials and requires fresh whole-round materialization for a topology retry",
                    ));
                }
                let reason = pre_ready_topology_reason(
                    first_error
                        .pre_ready_topology_outcome()
                        .expect("pre-ready topology outcome checked above"),
                );
                if let Err(error) = round_factory.permit_pre_ready_retry() {
                    record_pre_ready_effect_gate("rejected");
                    return Err(error);
                }
                record_pre_ready_effect_gate("permitted");
                record_pre_ready_replan(reason);
                let waiting_started_at = Instant::now();
                let fresh_topology = self
                    .backend_topology
                    .wait_for_eligible_after(first_revision, retry_deadline)
                    .map_err(|error| {
                        observe_waiting_for_backend(waiting_started_at.elapsed());
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::Failed,
                            error.to_string(),
                        )
                    })?;
                observe_waiting_for_backend(waiting_started_at.elapsed());
                let replan_started_at = Instant::now();
                let replacement = round_factory.replan(fresh_topology);
                observe_pre_ready_replan(replan_started_at.elapsed());
                let replacement = replacement?;
                let replacement_execution_id = execution_id_for_round(query_id, 2)?;
                self.execute_round(
                    query_id,
                    replacement_execution_id,
                    retry_deadline,
                    replacement,
                    None,
                    RoundCredentialLeaseSource::Frozen(QueryCredentialLeases::empty()),
                )
            }
        }
    }
}

fn pre_ready_topology_reason(outcome: PreReadyTopologyOutcome) -> &'static str {
    match outcome {
        PreReadyTopologyOutcome::BackendDraining { .. } => "backend_draining",
        PreReadyTopologyOutcome::BackendProcessMismatch { .. } => "backend_process_mismatch",
        PreReadyTopologyOutcome::BackendNotEligible { .. } => "backend_not_eligible",
        PreReadyTopologyOutcome::CompatibilityMismatch { .. } => "compatibility_mismatch",
    }
}

fn fail_closed_one_shot_topology_retry(
    intent: DistributedQueryIntent,
    error: DistributedQueryError,
) -> DistributedQueryError {
    if intent == DistributedQueryIntent::Write
        && let Some(outcome) = error.pre_ready_topology_outcome()
    {
        // A write request is one-shot. Until its DML owner can preserve the
        // exact target/base/publication binding and rebuild the complete write
        // layout under a positive zero-effect permit, it must not reuse this
        // request or silently enter the read-query replan controller.
        return DistributedQueryError::topology_retry_unsupported(
            outcome,
            format!(
                "distributed write cannot retry pre-ready topology change without a whole-round semantic binding and effect-free permit: {}",
                error.message()
            ),
        );
    }
    error
}

/// Establish one absolute execution deadline before the first distributed
/// round.  Every retry consumes this same monotonic budget; it must never
/// inherit a fresh `query_timeout` window merely because the layout changed.
fn statement_deadline_for_request(
    request: &DistributedQueryRequest,
) -> Result<Instant, DistributedQueryError> {
    if let Some(deadline) = request.deadline() {
        return Ok(deadline);
    }
    let timeout_ms = u64::try_from(request.options().timeout_ms().max(1)).map_err(|_| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "resolved query timeout does not fit an unsigned duration",
        )
    })?;
    Instant::now()
        .checked_add(Duration::from_millis(timeout_ms))
        .ok_or_else(|| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::Failed,
                "query deadline exceeds monotonic clock range",
            )
        })
}

fn emit_distributed_write_phase_marker(
    intent: DistributedQueryIntent,
    execution_id: QueryExecutionId,
    phase: &str,
    started: Instant,
    root_counts: Option<(u64, u64)>,
) {
    if intent != DistributedQueryIntent::Write
        || !cfg!(debug_assertions)
        || std::env::var_os("NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER").is_none()
    {
        return;
    }
    println!(
        "{}",
        distributed_write_phase_marker(
            execution_id,
            phase,
            started.elapsed().as_millis(),
            root_counts,
        )
    );
}

fn distributed_write_phase_marker(
    execution_id: QueryExecutionId,
    phase: &str,
    elapsed_ms: u128,
    root_counts: Option<(u64, u64)>,
) -> String {
    let query_id = execution_id.query_id();
    let marker = format!(
        "NOVAROCKS_DISTRIBUTED_WRITE_PHASE query_hi={} query_lo={} attempt={} phase={} elapsed_ms={}",
        query_id.high(),
        query_id.low(),
        execution_id.attempt_id().get(),
        phase,
        elapsed_ms,
    );
    root_counts.map_or(marker.clone(), |(batches, rows)| {
        format!("{marker} root_batches={batches} root_rows={rows}")
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StatisticsTaskCompletionFact {
    identity: TaskIdentity,
    terminal: bool,
    success_compatible_terminal: bool,
    state: Option<TaskState>,
    failure_cause: Option<TerminationDetail>,
}

fn statistics_tasks_are_terminal(round: &TaskRound) -> bool {
    round.execution().graph().tasks().all(|task| {
        round
            .execution()
            .task(task.task_id())
            .is_some_and(|remote| remote.is_terminal())
    })
}

fn statistics_all_success_error(round: &TaskRound) -> Option<DistributedQueryError> {
    let facts = round
        .execution()
        .graph()
        .tasks()
        .map(|task| {
            let remote = round.execution().task(task.task_id());
            StatisticsTaskCompletionFact {
                identity: task.identity(),
                terminal: remote.is_some_and(|remote| remote.is_terminal()),
                success_compatible_terminal: remote
                    .and_then(|remote| remote.status())
                    .is_some_and(|status| status.is_success_compatible_terminal()),
                state: remote.map(|remote| remote.task_state()),
                failure_cause: remote
                    .and_then(|remote| remote.status())
                    .and_then(|status| status.termination())
                    .cloned(),
            }
        })
        .collect::<Vec<_>>();
    statistics_all_success_failure_message(&facts, round.failure_cause()).map(|message| {
        DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
    })
}

fn statistics_all_success_failure_message(
    facts: &[StatisticsTaskCompletionFact],
    round_failure_cause: Option<&TerminationDetail>,
) -> Option<String> {
    let incompatible = facts
        .iter()
        .filter(|fact| !statistics_task_is_success_compatible(fact))
        .map(format_statistics_task_completion_fact)
        .collect::<Vec<_>>();
    if incompatible.is_empty() && round_failure_cause.is_none() {
        return None;
    }
    Some(format!(
        "statistics execution did not reach a success-compatible terminal set; \
         incompatible_tasks=[{}]; round_failure_cause={}",
        incompatible.join(", "),
        format_termination_detail(round_failure_cause),
    ))
}

/// Whether this task's terminal is compatible with the already-complete Root
/// result remaining successful.
///
/// The Root reached `FINISHED` before this predicate can run. A non-root task
/// may then be stood down with `UPSTREAM_NO_LONGER_NEEDED`: the Root aggregate
/// has already consumed every sender through exchange EOS, so that terminal
/// closes work the result no longer depends on. An abort, a failure, a missing
/// status, or a task that has not reached a terminal still refuses statistics
/// publication.
fn statistics_task_is_success_compatible(fact: &StatisticsTaskCompletionFact) -> bool {
    fact.terminal && fact.success_compatible_terminal
}

fn format_statistics_task_completion_fact(fact: &StatisticsTaskCompletionFact) -> String {
    let execution_id = fact.identity.query_execution_id();
    format!(
        "{{identity={{query_id={}, attempt_id={}, stage_id={}, task_id={}, \
         backend_process_id={}}}, terminal={}, state={}, failure_cause={}}}",
        execution_id.query_id(),
        execution_id.attempt_id().get(),
        fact.identity.stage_id(),
        fact.identity.task_id(),
        fact.identity.backend_process_id(),
        fact.terminal,
        fact.state.map_or("MISSING", TaskState::as_str),
        format_termination_detail(fact.failure_cause.as_ref()),
    )
}

fn format_termination_detail(detail: Option<&TerminationDetail>) -> String {
    match detail {
        Some(TerminationDetail::Canceled(reason)) => format!("CANCELED(reason={reason})"),
        Some(TerminationDetail::Aborted(cause)) => format!("ABORTED(cause={cause})"),
        Some(TerminationDetail::Failed(failure)) => format!(
            "FAILED(category={}, detail={})",
            failure.category(),
            failure.detail()
        ),
        None => "NONE".to_owned(),
    }
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

fn execution_id_for_round(
    query_id: QueryId,
    attempt: u32,
) -> Result<QueryExecutionId, DistributedQueryError> {
    QueryExecutionId::new(
        query_id,
        AttemptId::new(u64::from(attempt)).map_err(|error| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                error.to_string(),
            )
        })?,
    )
    .map_err(|error| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            error.to_string(),
        )
    })
}

/// Snapshot validation runs before the Init + ControlReady boundary.  Only a
/// concrete missing/replaced captured process becomes typed topology evidence;
/// revision-only and availability errors are not guessed into retryability.
fn pre_ready_topology_validation_error(
    error: BackendTopologyValidationError,
) -> DistributedQueryError {
    match error {
        BackendTopologyValidationError::GenerationChanged {
            backend_idx,
            captured_generation,
            ..
        }
        | BackendTopologyValidationError::TargetMissing {
            backend_idx,
            captured_generation,
            ..
        } => DistributedQueryError::pre_ready_topology(
            PreReadyTopologyOutcome::BackendNotEligible {
                backend_idx,
                process_id: captured_generation,
            },
            error.to_string(),
        ),
        other => failed(other.to_string()),
    }
}

/// A lifecycle transport failure remains terminal unless the FE-owned
/// membership authority can independently prove that a captured participant
/// process was replaced or is no longer eligible. This runs before
/// `ControlReady`, after the guarded old round has been aborted, and never
/// derives retryability from transport text.
fn reclassify_pre_ready_lifecycle_failure(
    topology: &dyn BackendTopologyPort,
    captured: &BackendTopologySnapshot,
    original: DistributedQueryError,
    observation_deadline: Instant,
) -> DistributedQueryError {
    if original.pre_ready_topology_outcome().is_some() {
        return original;
    }
    if !original.requires_pre_ready_topology_observation() {
        return original;
    }

    // The failed Init RPC itself is not retry evidence.  It can, however,
    // race the registered replacement's announce/heartbeat publication. Wait
    // for at most the already-budgeted Init RPC window, and only elevate the
    // failure if the membership owner independently proves one of the
    // captured processes became unavailable.
    let mut observed_revision = captured.revision();
    loop {
        let current = match topology.snapshot() {
            Ok(snapshot) => snapshot,
            Err(_) => return original,
        };
        observed_revision = observed_revision.max(current.revision());
        // A transport loss has no BE-provided draining disposition.  Do not
        // reinterpret the transient N-1 snapshot as an intentional scale-down
        // and replan onto it. Explicit `BackendDraining` evidence takes the
        // separate typed path above; this observer waits only for a replacement
        // that restores the captured participation capacity.
        if current.targets().len() < captured.targets().len() {
            if Instant::now() >= observation_deadline {
                return original;
            }
            let snapshot =
                match topology.wait_for_eligible_after(observed_revision, observation_deadline) {
                    Ok(snapshot) => snapshot,
                    Err(_) => return original,
                };
            observed_revision = snapshot.revision();
            continue;
        }
        match topology.validate_snapshot(captured) {
            Err(
                error @ (BackendTopologyValidationError::GenerationChanged { .. }
                | BackendTopologyValidationError::TargetMissing { .. }),
            ) => {
                let observed = pre_ready_topology_validation_error(error);
                let outcome = observed
                    .pre_ready_topology_outcome()
                    .expect("exact topology replacement is typed pre-ready evidence");
                tracing::info!(
                    original_error = %original,
                    observed_topology = %observed,
                    ?outcome,
                    "frontend reclassified a pre-ready lifecycle failure from exact backend topology evidence"
                );
                return DistributedQueryError::pre_ready_topology(
                    outcome,
                    format!(
                        "{original}; observed captured backend topology invalid before ControlReady: {observed}"
                    ),
                );
            }
            Err(BackendTopologyValidationError::ContentChangedWithoutRevision { .. })
            | Err(BackendTopologyValidationError::Unavailable(_)) => return original,
            Err(BackendTopologyValidationError::RevisionChanged {
                current_revision, ..
            }) => observed_revision = current_revision,
            Ok(()) => {}
        }
        if Instant::now() >= observation_deadline {
            return original;
        }
        let snapshot =
            match topology.wait_for_eligible_after(observed_revision, observation_deadline) {
                Ok(snapshot) => snapshot,
                Err(_) => return original,
            };
        observed_revision = snapshot.revision();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::{
        FrontendBackendSnapshot, FrontendDistributedQueryCoordinator, FrontendFragmentScheduler,
        QueryIdSource, ResultByteLimit, ResultPacketSequence, StatisticsTaskCompletionFact,
        TEST_RESULT_FETCH_BYTE_LIMIT, UniqueQueryIdSource, distributed_write_phase_marker,
        fail_closed_one_shot_topology_retry, pre_ready_topology_validation_error,
        statistics_all_success_failure_message,
    };
    use crate::common::backend_topology::{
        BackendTopologyPort, BackendTopologyValidationError, LiveBackendTarget,
    };
    use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::connector::{
        FixtureConnectorRegistry, FixtureControlResolver, test_request_context,
    };
    use crate::native::fragment_transport::{
        DynamicFilterRead, DynamicFilterReadError, ExpectedOutputSchemaView, FetchOutcome,
        FinalTaskInfoRead, FragmentDispatcher, RootResultOutcome,
    };
    use crate::query_execution::completion::{
        LogicalQueryReservation, PreReadyRetryBoundary, PreparedDistributedAttempt,
        PreparedDistributedAttemptFactory, PreparedDistributedQuery,
        PreparedDistributedRequestFactory, PreparedQueryCompletion,
        PreparedRetriableDistributedRequest,
    };
    use crate::query_execution::contract::{
        DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
        DistributedQueryIntent, DistributedQueryRequest, PreReadyTopologyOutcome,
        build_distributed_query_request_with_execution,
    };
    use crate::query_execution::preparation::{ScanPreparationOptions, prepare_fragments};
    use crate::topology::ClusterBackendService;
    use novarocks_execution::task_execution::domain::DomainVersion;
    use novarocks_execution::task_execution::{
        AbortCause, CancelReason, MaxWait, SafeDetail, TaskFailure, TaskFailureCategory,
        TaskIdentity, TaskState, TerminationDetail,
    };
    use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
    use novarocks_proto_codec::membership::{BackendProcessDescriptor, BackendReportedState};
    use novarocks_sql::test_support::{NativePreparationFixture, native_preparation_plan};
    use novarocks_types::identity::{StageId, TaskId};
    use novarocks_types::{AttemptId, QueryExecutionId};
    use novarocks_types::{
        BackendProcessId, ClusterRole, QueryId, QueryProcessNamespace, UniqueId,
    };
    use novarocks_version::native_build_identity;

    #[test]
    fn write_phase_marker_identifies_attempt_phase_and_bounded_root_counts() {
        let execution_id = QueryExecutionId::new(
            QueryId::new(7, 11),
            AttemptId::new(2).expect("nonzero attempt"),
        )
        .expect("nonzero execution identity");
        assert_eq!(
            distributed_write_phase_marker(execution_id, "root_eof", 37, Some((3, 19))),
            "NOVAROCKS_DISTRIBUTED_WRITE_PHASE query_hi=7 query_lo=11 attempt=2 phase=root_eof elapsed_ms=37 root_batches=3 root_rows=19"
        );
    }

    #[test]
    fn statistics_failure_preserves_every_incompatible_task_and_round_cause() {
        let execution_id = QueryExecutionId::new(
            QueryId::new(7, 11),
            AttemptId::new(2).expect("nonzero attempt"),
        )
        .expect("nonzero execution identity");
        let stage_id = StageId::new(3).expect("nonzero stage");
        let finished_identity = TaskIdentity::new(
            execution_id,
            stage_id,
            TaskId::new(1).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );
        let failed_identity = TaskIdentity::new(
            execution_id,
            stage_id,
            TaskId::new(2).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );
        let aborted_identity = TaskIdentity::new(
            execution_id,
            stage_id,
            TaskId::new(3).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );
        let task_failure = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::ResourceExhausted,
            SafeDetail::new("memory pool exhausted").expect("bounded safe detail"),
        ));
        let facts = vec![
            StatisticsTaskCompletionFact {
                identity: finished_identity,
                terminal: true,
                success_compatible_terminal: true,
                state: Some(TaskState::Finished),
                failure_cause: None,
            },
            StatisticsTaskCompletionFact {
                identity: failed_identity,
                terminal: true,
                success_compatible_terminal: false,
                state: Some(TaskState::Failed),
                failure_cause: Some(task_failure.clone()),
            },
            StatisticsTaskCompletionFact {
                identity: aborted_identity,
                terminal: true,
                success_compatible_terminal: false,
                state: Some(TaskState::Aborted),
                failure_cause: Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            },
        ];

        let message = statistics_all_success_failure_message(&facts, Some(&task_failure))
            .expect("non-finished tasks reject statistics completion");
        assert!(!message.contains("task_id=1,"));
        assert!(message.contains(&format!(
            "identity={{query_id={}, attempt_id=2, stage_id=3, task_id=2, backend_process_id={}}}, terminal=true, state=FAILED, failure_cause=FAILED(category=RESOURCE_EXHAUSTED, detail=memory pool exhausted)",
            execution_id.query_id(),
            failed_identity.backend_process_id(),
        )));
        assert!(message.contains(&format!(
            "identity={{query_id={}, attempt_id=2, stage_id=3, task_id=3, backend_process_id={}}}, terminal=true, state=ABORTED, failure_cause=ABORTED(cause=PEER_TASK_FAILED)",
            execution_id.query_id(),
            aborted_identity.backend_process_id(),
        )));
        assert!(message.ends_with(
            "round_failure_cause=FAILED(category=RESOURCE_EXHAUSTED, detail=memory pool exhausted)"
        ));
        assert!(
            statistics_all_success_failure_message(&facts[..1], None).is_none(),
            "a FINISHED task set with no round failure is successful"
        );
    }

    #[test]
    fn statistics_accepts_only_the_closed_success_compatible_cancel_terminal() {
        let execution_id = QueryExecutionId::new(
            QueryId::new(13, 17),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero execution identity");
        let stage_id = StageId::new(2).expect("nonzero stage");
        let finished_root = StatisticsTaskCompletionFact {
            identity: TaskIdentity::new(
                execution_id,
                stage_id,
                TaskId::new(1).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            terminal: true,
            success_compatible_terminal: true,
            state: Some(TaskState::Finished),
            failure_cause: None,
        };
        let canceled_producer = StatisticsTaskCompletionFact {
            identity: TaskIdentity::new(
                execution_id,
                stage_id,
                TaskId::new(2).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            terminal: true,
            success_compatible_terminal: true,
            state: Some(TaskState::Canceled),
            failure_cause: Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
        };
        assert!(
            statistics_all_success_failure_message(
                &[finished_root.clone(), canceled_producer],
                None,
            )
            .is_none(),
            "a producer stood down after Root EOF is success-compatible"
        );

        let aborted_producer = StatisticsTaskCompletionFact {
            identity: TaskIdentity::new(
                execution_id,
                stage_id,
                TaskId::new(3).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            terminal: true,
            success_compatible_terminal: false,
            state: Some(TaskState::Aborted),
            failure_cause: Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
        };
        let message =
            statistics_all_success_failure_message(&[finished_root, aborted_producer], None)
                .expect("query cancellation is not a success-compatible stand-down");
        assert!(message.contains("ABORTED(cause=QUERY_FAILED)"));
    }

    #[test]
    fn statistics_does_not_classify_a_cancel_until_it_reaches_terminal() {
        let execution_id = QueryExecutionId::new(
            QueryId::new(19, 23),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero execution identity");
        let canceling = StatisticsTaskCompletionFact {
            identity: TaskIdentity::new(
                execution_id,
                StageId::new(2).expect("nonzero stage"),
                TaskId::new(1).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            terminal: false,
            success_compatible_terminal: false,
            state: Some(TaskState::Canceling),
            failure_cause: Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
        };
        let message = statistics_all_success_failure_message(&[canceling], None)
            .expect("a canceling task is not yet classifiable as success");
        assert!(message.contains("terminal=false"));
        assert!(message.contains("state=CANCELING"));
    }

    #[test]
    fn missing_captured_process_is_typed_pre_ready_not_eligible() {
        let process_id = BackendProcessId::new_v7();
        let error =
            pre_ready_topology_validation_error(BackendTopologyValidationError::TargetMissing {
                backend_idx: 2,
                captured_generation: process_id,
                captured_revision: 7,
                current_revision: 8,
            });
        assert_eq!(
            error.pre_ready_topology_outcome(),
            Some(PreReadyTopologyOutcome::BackendNotEligible {
                backend_idx: 2,
                process_id,
            })
        );
    }

    #[test]
    fn one_shot_write_rejects_pre_ready_topology_without_a_replan_owner() {
        let process_id = BackendProcessId::new_v7();
        let original = DistributedQueryError::pre_ready_topology(
            PreReadyTopologyOutcome::BackendDraining {
                backend_idx: 1,
                process_id,
            },
            "backend is draining",
        );
        let outcome = original
            .pre_ready_topology_outcome()
            .expect("typed outcome is retained");
        let error = fail_closed_one_shot_topology_retry(DistributedQueryIntent::Write, original);

        assert_eq!(
            error.kind(),
            DistributedQueryErrorKind::TopologyRetryUnsupported
        );
        assert_eq!(error.pre_ready_topology_outcome(), Some(outcome));
    }

    #[test]
    fn unique_query_id_source_uses_one_namespace_with_continuous_positive_sequences() {
        let namespace = QueryProcessNamespace::new(0xfedc_ba98_7654_3210);
        let source = UniqueQueryIdSource::new(namespace);

        let first = source.next_query_id().expect("first allocation");
        let second = source.next_query_id().expect("second allocation");

        assert_eq!(
            first
                .process_attribution()
                .expect("first attribution")
                .namespace(),
            namespace
        );
        assert_eq!(
            first
                .process_attribution()
                .expect("first attribution")
                .sequence()
                .get(),
            1
        );
        assert_eq!(
            second
                .process_attribution()
                .expect("second attribution")
                .namespace(),
            namespace
        );
        assert_eq!(
            second
                .process_attribution()
                .expect("second attribution")
                .sequence()
                .get(),
            2
        );
    }

    #[test]
    fn unique_query_id_sources_keep_injected_process_namespaces_distinct() {
        let first = UniqueQueryIdSource::new(QueryProcessNamespace::new(11));
        let second = UniqueQueryIdSource::new(QueryProcessNamespace::new(12));

        let first_id = first.next_query_id().expect("first namespace allocation");
        let second_id = second.next_query_id().expect("second namespace allocation");

        assert_eq!(first_id.low(), second_id.low());
        assert_ne!(first_id.high(), second_id.high());
        assert_ne!(first_id, second_id);
    }

    #[test]
    fn unique_query_id_source_fails_closed_after_sequence_exhaustion() {
        let source = UniqueQueryIdSource::with_last_issued_sequence(
            QueryProcessNamespace::new(13),
            i64::MAX as u64 - 1,
        );

        let final_id = source.next_query_id().expect("final sequence allocation");
        assert_eq!(
            final_id
                .process_attribution()
                .expect("final attribution")
                .sequence()
                .get(),
            i64::MAX as u64
        );
        let error = source.next_query_id().expect_err("exhaustion must fail");
        assert_eq!(error.kind(), DistributedQueryErrorKind::Failed);
        assert_eq!(
            error.message(),
            "frontend query id local sequence is exhausted"
        );
    }

    struct FailingAfterStartDispatcher;

    impl FragmentDispatcher for FailingAfterStartDispatcher {
        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: UniqueId,
            _max_wait_ms: i64,
            _expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
        ) -> Result<FetchOutcome, String> {
            Err("test fetch failure after retry stage/start".to_string())
        }

        fn backend_count(&self) -> usize {
            1
        }
    }

    struct RecordingRetryFactory {
        permits: Arc<AtomicUsize>,
        control_ready_closures: Arc<AtomicUsize>,
        stage_or_start_closures: Arc<AtomicUsize>,
        replanned_topologies:
            Arc<Mutex<Vec<crate::common::backend_topology::BackendTopologySnapshot>>>,
    }

    impl PreparedDistributedAttemptFactory for RecordingRetryFactory {
        fn instantiate(
            &mut self,
            topology: crate::common::backend_topology::BackendTopologySnapshot,
        ) -> Result<PreparedDistributedAttempt, DistributedQueryError> {
            self.replanned_topologies
                .lock()
                .expect("replanned topologies")
                .push(topology.clone());
            Ok(PreparedDistributedAttempt::new(
                fresh_result_request(topology)?,
                PreparedQueryCompletion::result(),
            ))
        }
    }

    impl PreparedDistributedRequestFactory for RecordingRetryFactory {
        fn replan(
            &mut self,
            topology: crate::common::backend_topology::BackendTopologySnapshot,
        ) -> Result<DistributedQueryRequest, DistributedQueryError> {
            self.replanned_topologies
                .lock()
                .expect("replanned topologies")
                .push(topology.clone());
            fresh_result_request(topology)
        }
    }

    impl PreReadyRetryBoundary for RecordingRetryFactory {
        fn permit_pre_ready_retry(&self) -> Result<(), DistributedQueryError> {
            self.permits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn close_after_control_ready(&self) {
            self.control_ready_closures.fetch_add(1, Ordering::SeqCst);
        }

        fn close_after_stage_or_start(&self) {
            self.stage_or_start_closures.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn descriptor(process_id: BackendProcessId, endpoint: SocketAddr) -> BackendProcessDescriptor {
        BackendProcessDescriptor::new(
            process_id,
            QueryControlEndpoint::new(endpoint.ip().to_string(), endpoint.port())
                .expect("test endpoint"),
            "test-deployment",
            native_build_identity(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        )
        .expect("test descriptor")
    }

    fn verify(
        topology: &ClusterBackendService,
        descriptor: &BackendProcessDescriptor,
        now_ms: i64,
    ) {
        topology.record_heartbeat_success(
            descriptor.process_id().expect("descriptor process id"),
            descriptor.clone(),
            BackendReportedState::Running,
            2,
            novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes(
                [0x61; 16],
            )
            .expect("nonzero epoch"),
            now_ms,
        );
    }

    fn fresh_result_request(
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<DistributedQueryRequest, DistributedQueryError> {
        let plan = native_preparation_plan(NativePreparationFixture::ResultOutput)
            .expect("sealed result fixture");
        let registry = FixtureConnectorRegistry::new();
        let controls = FixtureControlResolver::new(registry.clone());
        let prepared = prepare_fragments(
            &plan,
            &controls,
            &test_request_context(),
            None,
            None,
            ScanPreparationOptions::single_backend_fixture(),
        )
        .expect("prepared result fixture");
        let encoding =
            crate::query_execution::post_compile::NativeFragmentEncodingInput::new(prepared);
        let native = encoding
            .native_attachment_for_test(
                [novarocks_proto_models::plan::PlanFragment {
                    fragment_id: 7,
                    // The task protocol refuses a fragment plan with no sink, so a
                    // fixture without one would fail at graph assembly and never
                    // reach the behaviour these tests are about.
                    sink: Some(novarocks_proto_models::plan::DataSink {
                        kind: Some(novarocks_proto_models::plan::data_sink::Kind::Result(true)),
                    }),
                    ..Default::default()
                }],
                &BTreeSet::from([7]),
            )
            .expect("native fragment fixture");
        let cancellation = QueryCancellationSource::new();
        let execution = crate::common::admitted_query_context::QueryExecutionContext::new(
            ClusterRole::Fe,
            topology,
            // Short on purpose. These fixtures point at endpoints nothing
            // listens on, so an attempt that reaches the task substrate ends
            // at its own deadline; the budget only has to outlast preparation.
            Some(Instant::now() + Duration::from_secs(1)),
            cancellation.view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        );
        build_distributed_query_request_with_execution(
            encoding,
            native,
            None,
            DistributedQueryIntent::Result,
            &execution,
        )
    }

    #[test]
    fn a_drain_never_inherits_the_remainder_of_a_long_statement_timeout() {
        // The client-visible answer is linearized before the drain starts, so
        // every moment the drain spends is a moment the caller waits for a
        // result already in hand. A query that finished in a second against
        // one stuck backend must return in about the drain budget, not in five
        // minutes.
        let now = Instant::now();
        let statement_deadline = now + Duration::from_secs(300);
        let budget = Duration::from_secs(15);

        let deadline = super::drain_deadline(statement_deadline, budget, now);
        assert_eq!(
            deadline - now,
            budget,
            "the drain's own budget bounds it, not the statement's remaining time"
        );

        // A statement already near its end has no time left to lend: the drain
        // cannot extend a query past the deadline its caller was promised.
        let nearly_over = now + Duration::from_millis(50);
        assert_eq!(
            super::drain_deadline(nearly_over, budget, now),
            nearly_over,
            "the statement deadline still caps the drain"
        );

        // And an already-expired statement yields no drain time at all rather
        // than wrapping into a long wait.
        let expired = now - Duration::from_secs(1);
        assert!(
            super::drain_deadline(expired, budget, now) <= now,
            "an expired statement leaves the drain no budget"
        );
    }

    /// A killed statement's worker must not wait out the pre-establish
    /// membership observation.
    ///
    /// `KILL QUERY` is withheld from the client until this worker unwinds, so
    /// a pre-ControlReady failure that stops to ask the membership owner
    /// whether a backend was replaced spends that entire window inside the
    /// client's wait for its own interrupt. On a healthy cluster no
    /// replacement ever arrives, so the window is spent in full -- and the
    /// window is the fifteen-second establish wait, which is exactly the
    /// interrupt latency a real 1FE+3BE cluster reported for a statement
    /// killed while its `EstablishQueryContext` was still in flight.
    ///
    /// Both directions are asserted, and the uncancelled one comes first
    /// because it is the direction that can pass for the wrong reason. A
    /// change that simply stopped observing would satisfy the cancelled half
    /// and break the other, and that observation is the only evidence a
    /// pre-establish replan is ever allowed to rest on.
    #[test]
    fn a_cancelled_attempt_does_not_wait_out_the_pre_establish_membership_observation() {
        let endpoint: SocketAddr = "127.0.0.1:19061".parse().expect("test endpoint");
        let backend = descriptor(BackendProcessId::new_v7(), endpoint);
        let topology = Arc::new(ClusterBackendService::new_transient_for_test(1));
        topology
            .record_announce(backend.clone(), BackendReportedState::Running)
            .expect("announce the only backend");
        verify(topology.as_ref(), &backend, 1);
        let captured = topology.snapshot().expect("captured topology");

        // Short enough to keep this test quick, long enough that spending it
        // is unmistakable. Production's window is the establish wait.
        const OBSERVATION: Duration = Duration::from_secs(1);
        const MESSAGE: &str = "query cancelled while fetching result";

        let cancellation = QueryCancellationSource::new();
        let view = cancellation.view();

        // Nothing has cancelled this attempt, so the membership owner is
        // asked. This topology's revision never advances, so being asked
        // means the whole window is spent before the failure is judged to be
        // this attempt's own.
        let started = Instant::now();
        let observed = super::judge_pre_ready_task_round_failure(
            topology.as_ref(),
            super::TaskRoundFailureClassification {
                before_contexts_established: true,
                captured: &captured,
                observation_deadline: started + OBSERVATION,
                cancellation: &view,
            },
            MESSAGE,
        );
        let uncancelled = started.elapsed();
        assert!(
            observed.is_none(),
            "an unchanged topology proves no replacement, so the failure is this attempt's own"
        );
        assert!(
            uncancelled >= OBSERVATION.mul_f32(0.75),
            "an uncancelled pre-establish failure must still consult the membership owner, \
             yet it was judged after only {uncancelled:?}"
        );

        // Latch the cancellation the statement source latches for a
        // `KILL QUERY`, and ask again. Nothing about the topology changed;
        // only the answer's already being decided has.
        cancellation.request(QueryCancellationReason::ExplicitKill {
            requester_connection_id: 7,
        });
        assert!(view.is_cancelled(), "the statement source latches the kill");

        let started = Instant::now();
        let cancelled = super::judge_pre_ready_task_round_failure(
            topology.as_ref(),
            super::TaskRoundFailureClassification {
                before_contexts_established: true,
                captured: &captured,
                observation_deadline: started + OBSERVATION,
                cancellation: &view,
            },
            MESSAGE,
        );
        let elapsed = started.elapsed();
        assert!(
            cancelled.is_none(),
            "a cancelled attempt is never reclassified into topology-retry evidence, \
             because replacement must never re-run a statement the client killed"
        );
        assert!(
            elapsed < OBSERVATION / 4,
            "a killed statement's worker must unwind without waiting out the membership \
             observation, yet it was judged after {elapsed:?}"
        );
    }

    #[test]
    fn a_replaced_captured_process_instantiates_one_attempt_before_establish() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let _runtime_guard = runtime.enter();
        let endpoint: SocketAddr = "127.0.0.1:19041".parse().expect("test endpoint");
        let old = descriptor(BackendProcessId::new_v7(), endpoint);
        let replacement = descriptor(BackendProcessId::new_v7(), endpoint);
        let topology = Arc::new(ClusterBackendService::new_transient_for_test(1));
        topology
            .record_announce(old.clone(), BackendReportedState::Running)
            .expect("initial announce");
        verify(topology.as_ref(), &old, 1);
        let first_snapshot = topology.snapshot().expect("initial topology");
        let old_scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(first_snapshot.targets().to_vec())
                .expect("old scheduler"),
        );

        let replacement_for_scheduler = replacement.clone();
        let replacement_scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(vec![LiveBackendTarget::new(
                0,
                replacement_for_scheduler,
                novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes(
                    [0x61; 16],
                )
                .expect("nonzero epoch"),
            )])
            .expect("replacement scheduler"),
        );
        let coordinator =
            FrontendDistributedQueryCoordinator::new_for_test_with_backend_sequence_and_topology(
                QueryId::new(7, 11),
                vec![old_scheduler, replacement_scheduler],
                Arc::new(FailingAfterStartDispatcher),
                NonZeroUsize::new(1).expect("nonzero workers"),
                Arc::new(()),
                Arc::clone(&topology) as crate::common::backend_topology::BackendTopologyService,
            );
        // The membership owner replaces the captured process. Published before
        // the attempt starts because that is the only ordering a test can pin;
        // what matters is that replacement rests on this fact rather than on a
        // transport's own report of it.
        topology
            .record_announce(replacement.clone(), BackendReportedState::Running)
            .expect("replacement announce");
        verify(topology.as_ref(), &replacement, 2);
        let permits = Arc::new(AtomicUsize::new(0));
        let control_ready_closures = Arc::new(AtomicUsize::new(0));
        let stage_or_start_closures = Arc::new(AtomicUsize::new(0));
        let replanned_topologies = Arc::new(Mutex::new(Vec::new()));
        let operation = PreparedDistributedQuery::new(
            fresh_result_request(first_snapshot.clone()).expect("first request"),
            PreparedQueryCompletion::result(),
            LogicalQueryReservation::for_test(QueryId::new(7, 11)),
        )
        .with_attempt_factory(Box::new(RecordingRetryFactory {
            permits: Arc::clone(&permits),
            control_ready_closures: Arc::clone(&control_ready_closures),
            stage_or_start_closures: Arc::clone(&stage_or_start_closures),
            replanned_topologies: Arc::clone(&replanned_topologies),
        }));

        let error = coordinator
            .execute_prepared(operation)
            .expect_err("the replacement attempt has no backend to reach");
        // The replacement attempt runs on the task substrate against an endpoint
        // nothing listens on, so it ends at its own deadline. What this test
        // is about happened before that: one permit, one replacement, onto the
        // process the membership owner named.
        assert!(
            error.message().contains("query timed out after"),
            "actual: {}",
            error.message()
        );
        assert_eq!(permits.load(Ordering::SeqCst), 1);
        // Neither gate may close: no query context was ever established, so
        // the window in which a replacement is still legal never ended.
        assert_eq!(control_ready_closures.load(Ordering::SeqCst), 0);
        assert_eq!(stage_or_start_closures.load(Ordering::SeqCst), 0);
        let replanned = replanned_topologies.lock().expect("replanned topologies");
        assert_eq!(replanned.len(), 1);
        assert!(replanned[0].revision() > first_snapshot.revision());
        assert_eq!(
            replanned[0].targets()[0]
                .process_id()
                .expect("replacement process id"),
            replacement.process_id().expect("replacement process id"),
        );
    }

    /// The same replan through the raw entrypoint, which owns the write
    /// outcome boundary.
    ///
    /// Its subject is that a replan does not lose that boundary, not how the
    /// replan was triggered, so it follows the surviving trigger for the same
    /// reason the test above does.
    #[test]
    fn raw_replan_on_a_replaced_process_keeps_the_write_outcome_boundary() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let _runtime_guard = runtime.enter();
        let endpoint: SocketAddr = "127.0.0.1:19043".parse().expect("test endpoint");
        let old = descriptor(BackendProcessId::new_v7(), endpoint);
        let replacement = descriptor(BackendProcessId::new_v7(), endpoint);
        let topology = Arc::new(ClusterBackendService::new_transient_for_test(1));
        topology
            .record_announce(old.clone(), BackendReportedState::Running)
            .expect("initial announce");
        verify(topology.as_ref(), &old, 1);
        let first_snapshot = topology.snapshot().expect("initial topology");
        let old_scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(first_snapshot.targets().to_vec())
                .expect("old scheduler"),
        );
        let replacement_scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(vec![LiveBackendTarget::new(
                0,
                replacement.clone(),
                novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes(
                    [0x62; 16],
                )
                .expect("nonzero replacement epoch"),
            )])
            .expect("replacement scheduler"),
        );
        let coordinator =
            FrontendDistributedQueryCoordinator::new_for_test_with_backend_sequence_and_topology(
                QueryId::new(7, 13),
                vec![old_scheduler, replacement_scheduler],
                Arc::new(FailingAfterStartDispatcher),
                NonZeroUsize::new(1).expect("nonzero workers"),
                Arc::new(()),
                Arc::clone(&topology) as crate::common::backend_topology::BackendTopologyService,
            );
        topology
            .record_announce(replacement.clone(), BackendReportedState::Running)
            .expect("replacement announce");
        verify(topology.as_ref(), &replacement, 2);
        let permits = Arc::new(AtomicUsize::new(0));
        let control_ready_closures = Arc::new(AtomicUsize::new(0));
        let stage_or_start_closures = Arc::new(AtomicUsize::new(0));
        let replanned_topologies = Arc::new(Mutex::new(Vec::new()));
        let operation = PreparedRetriableDistributedRequest::new(
            fresh_result_request(first_snapshot.clone()).expect("first request"),
            Box::new(RecordingRetryFactory {
                permits: Arc::clone(&permits),
                control_ready_closures: Arc::clone(&control_ready_closures),
                stage_or_start_closures: Arc::clone(&stage_or_start_closures),
                replanned_topologies: Arc::clone(&replanned_topologies),
            }),
        );

        let error = match coordinator.execute_prepared_raw(operation) {
            Ok(_) => panic!("the replanned raw round has no backend to reach"),
            Err(error) => error,
        };
        assert!(
            error.message().contains("query timed out after"),
            "actual: {}",
            error.message()
        );
        assert_eq!(permits.load(Ordering::SeqCst), 1);
        let replanned = replanned_topologies.lock().expect("replanned topologies");
        assert_eq!(replanned.len(), 1);
        assert!(replanned[0].revision() > first_snapshot.revision());
        assert_eq!(
            replanned[0].targets()[0]
                .process_id()
                .expect("replacement process id"),
            replacement.process_id().expect("replacement process id"),
        );
    }

    /// Both gates are evidence, not milestones a code path passes.
    ///
    /// This round reaches the task substrate and never gets an answer from it,
    /// so no query context is established and no task is created. Neither gate
    /// may close: closing one here would end the window in which a replaced
    /// backend can still be replanned onto, on the strength of having asked
    /// rather than having been answered.
    ///
    /// The other half -- that both gates do close once the answers arrive --
    /// is asserted against the state machine itself in
    /// `task_execution::tests::the_two_start_gates_are_observations_of_acknowledgements_not_of_sending`,
    /// because a fixture-injected transport cannot answer the task protocol:
    /// it speaks over a real connection.
    #[test]
    fn the_round_retry_boundary_stays_open_while_no_backend_has_answered() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let _runtime_guard = runtime.enter();
        let endpoint: SocketAddr = "127.0.0.1:19042".parse().expect("test endpoint");
        let descriptor = descriptor(BackendProcessId::new_v7(), endpoint);
        let topology = Arc::new(ClusterBackendService::new_transient_for_test(1));
        topology
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .expect("initial announce");
        verify(topology.as_ref(), &descriptor, 1);
        let snapshot = topology.snapshot().expect("eligible topology");
        let scheduler = FrontendFragmentScheduler::new(
            FrontendBackendSnapshot::from_live_targets(snapshot.targets().to_vec())
                .expect("scheduler"),
        );
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test_with_topology(
            QueryId::new(7, 12),
            scheduler,
            Arc::new(FailingAfterStartDispatcher),
            NonZeroUsize::new(1).expect("nonzero workers"),
            Arc::new(()),
            Arc::clone(&topology) as crate::common::backend_topology::BackendTopologyService,
        );
        let control_ready_closures = Arc::new(AtomicUsize::new(0));
        let stage_or_start_closures = Arc::new(AtomicUsize::new(0));
        let operation = PreparedDistributedQuery::new(
            fresh_result_request(snapshot).expect("first request"),
            PreparedQueryCompletion::result(),
            LogicalQueryReservation::for_test(QueryId::new(7, 12)),
        )
        .with_attempt_factory(Box::new(RecordingRetryFactory {
            permits: Arc::new(AtomicUsize::new(0)),
            control_ready_closures: Arc::clone(&control_ready_closures),
            stage_or_start_closures: Arc::clone(&stage_or_start_closures),
            replanned_topologies: Arc::new(Mutex::new(Vec::new())),
        }));

        let error = coordinator
            .execute_prepared(operation)
            .expect_err("the round has no backend to reach");
        assert!(
            error.message().contains("query timed out after"),
            "actual: {}",
            error.message()
        );
        assert_eq!(control_ready_closures.load(Ordering::SeqCst), 0);
        assert_eq!(stage_or_start_closures.load(Ordering::SeqCst), 0);
    }

    /// A root result transport whose every poll answers only when this test
    /// says so, so that "a poll is in flight" is a state the test holds
    /// rather than one it races.
    struct ScriptedRootResult {
        /// One answer per poll, in the order they are given.
        answers: Mutex<std::collections::VecDeque<RootResultOutcome>>,
        /// Received once per poll before it answers.
        releases: Mutex<std::sync::mpsc::Receiver<()>>,
        polls: AtomicUsize,
        acknowledgements: Mutex<Vec<Option<ResultPacketSequence>>>,
    }

    impl super::TaskResultTransport for ScriptedRootResult {
        fn fetch_root_result(
            &self,
            _root_task: TaskIdentity,
            _max_wait: MaxWait,
            acknowledged: Option<ResultPacketSequence>,
            _max_result_bytes: ResultByteLimit,
            _expected_output_schema: Option<novarocks_execution::exec::chunk::ChunkSchemaRef>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<RootResultOutcome, String>>
                    + Send
                    + 'static,
            >,
        > {
            let answer = self
                .releases
                .lock()
                .expect("scripted release lock")
                .recv()
                .map_err(|_| "the test stopped releasing polls".to_owned())
                .and_then(|()| {
                    self.polls.fetch_add(1, Ordering::SeqCst);
                    self.acknowledgements
                        .lock()
                        .expect("scripted acknowledgement lock")
                        .push(acknowledged);
                    self.answers
                        .lock()
                        .expect("scripted answer lock")
                        .pop_front()
                        .ok_or_else(|| "the script ran out of answers".to_owned())
                });
            Box::pin(async move { answer })
        }

        fn final_task_info(&self, _identity: TaskIdentity) -> Result<FinalTaskInfoRead, String> {
            Err("this transport answers only root result polls".to_owned())
        }

        fn dynamic_filters(
            &self,
            _identity: TaskIdentity,
            _acknowledged: Option<DomainVersion>,
        ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
            Err(DynamicFilterReadError::Refused(
                "this transport answers only root result polls".to_owned(),
            ))
        }
    }

    fn root_task_for_test() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(
                QueryId::new(0x1234, 0x5678),
                AttemptId::new(1).expect("attempt one is nonzero"),
            )
            .expect("a nonzero query id"),
            StageId::new(1).expect("a nonzero stage id"),
            TaskId::new(1).expect("a nonzero task id"),
            BackendProcessId::new_v7(),
        )
    }

    /// Waits for the poller to answer, bounded, so a failure is a failure
    /// rather than a hang.
    fn answer_within(
        polls: &mut super::RootResultPolls,
        budget: Duration,
    ) -> Option<Result<RootResultOutcome, String>> {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(answer) = polls.take() {
                return Some(answer);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The defect this pins: one root result poll asks a backend to hold its
    /// answer for up to `MAX_ROOT_RESULT_WAIT`, while the attempt owner must
    /// keep turning its task state machine. When the owner waited out a poll
    /// it dispatched nothing, so every decision made elsewhere meanwhile
    /// waited for the poll instead of for its own facts. A distributed
    /// `SELECT` opens its producers' exchange edges
    /// exactly there: each edge-open decision cost one whole poll wait, and
    /// the cost was invisible as anything but latency. Measured on a 1FE+3BE
    /// cluster, every statement of the `filter` suite took ~0.85 s of which
    /// ~0.05 s was work, and the suite took 92 s against its baseline's 4 s.
    ///
    /// Answers must therefore reach the loop without the loop waiting for
    /// one. A following poll must also wait for the owner to accept the exact
    /// prior sequence; enqueueing an answer is not an acknowledgement.
    #[test]
    fn root_result_polls_answer_a_loop_that_never_waits_for_one() {
        let (release, releases) = std::sync::mpsc::channel();
        let transport = Arc::new(ScriptedRootResult {
            answers: Mutex::new(
                [
                    RootResultOutcome::NotReady,
                    RootResultOutcome::EndOfStreamPending { packet_sequence: 7 },
                    RootResultOutcome::EndOfStream { packet_sequence: 7 },
                ]
                .into_iter()
                .collect(),
            ),
            releases: Mutex::new(releases),
            polls: AtomicUsize::new(0),
            acknowledgements: Mutex::new(Vec::new()),
        });
        let wake = Arc::new(crate::task_execution::status_intake::CountingWake::default());
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("root result poll runtime");
        let mut polls = super::RootResultPolls::start(
            Arc::clone(&transport) as Arc<dyn super::TaskResultTransport>,
            root_task_for_test(),
            Arc::new(novarocks_execution::exec::chunk::ChunkSchema::empty()),
            Instant::now() + Duration::from_secs(60),
            ResultByteLimit::new(TEST_RESULT_FETCH_BYTE_LIMIT)
                .expect("the test result byte limit is nonzero"),
            Arc::clone(&wake) as Arc<dyn crate::task_execution::status_intake::StatusIntakeWake>,
            crate::native::data_runtime::FrontendDataRuntime::new(runtime.handle().clone()),
        )
        .expect("the poller starts");

        // A poll is in flight and this test is holding its answer. Taking
        // from the poller answers immediately anyway, and that is the whole
        // property: the loop is free to turn its state machine.
        assert!(
            polls.take().is_none(),
            "a poll in flight must not hand the loop an answer"
        );
        assert_eq!(wake.count(), 0, "nothing has been answered yet");

        // The first answer says nothing is ready. It still wakes the loop,
        // and the poller asks again on its own.
        release
            .send(())
            .expect("the poller is waiting for a release");
        let first = answer_within(&mut polls, Duration::from_secs(10))
            .expect("the first answer reaches the loop");
        assert!(
            matches!(first, Ok(RootResultOutcome::NotReady)),
            "actual: {first:?}"
        );
        assert!(wake.count() >= 1, "an answer wakes the loop");

        release.send(()).expect("the poller polls again by itself");
        let second = answer_within(&mut polls, Duration::from_secs(10))
            .expect("the second answer reaches the loop");
        assert!(
            matches!(
                second,
                Ok(RootResultOutcome::EndOfStreamPending { packet_sequence: 7 })
            ),
            "actual: {second:?}"
        );
        assert_eq!(
            transport.polls.load(Ordering::SeqCst),
            2,
            "the next poll must wait until the owner accepts pending EOS"
        );
        polls
            .acknowledge(7)
            .expect("the attempt owner acknowledges pending EOS");
        release
            .send(())
            .expect("the accepted EOS acknowledgement starts the final poll");
        let third = answer_within(&mut polls, Duration::from_secs(10))
            .expect("the final answer reaches the loop");
        assert!(
            matches!(
                third,
                Ok(RootResultOutcome::EndOfStream { packet_sequence: 7 })
            ),
            "actual: {third:?}"
        );

        // Nothing is polled after the end of the stream. The poller has let
        // go of the transport, which is what says it stopped rather than
        // merely paused.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Arc::strong_count(&transport) > 1 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            Arc::strong_count(&transport),
            1,
            "the poller must stop itself once the read is over"
        );
        assert_eq!(
            transport.polls.load(Ordering::SeqCst),
            3,
            "exactly the three polls this test released"
        );
        assert_eq!(
            *transport
                .acknowledgements
                .lock()
                .expect("scripted acknowledgement lock"),
            vec![None, None, Some(ResultPacketSequence::new(7))]
        );
    }
}

/// How long the coordinator parks when one turn moved nothing at all.
///
/// Only a pacing bound: every wait ends early when the transport wakes the
/// runner, and every deadline is checked before the next turn.
const TASK_ROUND_IDLE_WAIT: Duration = Duration::from_millis(5);

/// The longest one root result poll may block a backend.
///
/// It bounds how long a backend holds one request, and nothing else: the polls
/// run on [`RootResultPolls`], so the attempt's own loop keeps settling
/// acknowledgements and opening edges throughout one. What the number still
/// buys is diagnostics and shutdown -- the interval at which a poll that
/// answers nothing refreshes the wait facts, and the longest a poller lingers
/// after the loop stops reading it.
///
/// It must not be read as a bound on the loop's responsiveness. It was one
/// once, and being one is what made every statement of a distributed suite
/// wait a poll per edge-open decision.
const MAX_ROOT_RESULT_WAIT: Duration = Duration::from_millis(200);

/// How long an attempt may make no observable progress before it says, in the
/// log, which facts its completion is still waiting on.
const TASK_ROUND_WAIT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// What the last root result poll answered.
///
/// `NotPolledYet` is a distinct answer on purpose: the result plane refuses a
/// poll for a task whose creation has not been acknowledged, so the loop does
/// not make one. "Never polled" and "polled and told nothing" are different
/// faults and must not read alike in a log.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
enum RootResultPoll {
    #[default]
    NotPolledYet,
    NotReady,
    Packet(u64),
    EndOfStream(u64),
}

impl std::fmt::Display for RootResultPoll {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPolledYet => formatter.write_str("not polled yet"),
            Self::NotReady => formatter.write_str("not ready"),
            Self::Packet(sequence) => write!(formatter, "packet {sequence}"),
            Self::EndOfStream(sequence) => write!(formatter, "end of stream at {sequence}"),
        }
    }
}

/// Every fact one attempt's client-visible completion is still waiting on.
///
/// A read completes only when the root task published `FINISHED` and this
/// frontend consumed the end of the root result stream. Both halves tolerate
/// absence by design -- a fact that has not arrived keeps the loop turning
/// rather than failing it -- which is exactly why the loop has to be able to
/// name the one that is missing. Without it, an attempt that waited out its
/// whole statement budget reported only "query timed out", a message that
/// names no fact and leaves the difference between "the root never finished",
/// "the stream never ended" and "the root's create was never acknowledged, so
/// nothing was ever polled" to be recovered from a cluster run per candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
struct TaskRoundWaitFacts {
    contexts_established: bool,
    tasks_created: bool,
    read: crate::task_execution::completion::ReadVerdict,
    write: Option<WriteVerdict>,
    /// `None` means no task of the root's id is in this attempt's state at
    /// all, which is a different fault from a root that is still creating.
    root_create: Option<RemoteTaskState>,
    last_root_poll: RootResultPoll,
    packets: usize,
    tasks: Vec<(
        TaskIdentity,
        RemoteTaskState,
        novarocks_execution::task_execution::TaskState,
        bool,
    )>,
}

impl std::fmt::Display for TaskRoundWaitFacts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "read={:?} write={} contexts_established={} tasks_created={} root_create={} \
             last_root_poll={} packets={} tasks=[",
            self.read,
            self.write.map_or_else(
                || "not-applicable".to_owned(),
                |verdict| verdict.to_string(),
            ),
            self.contexts_established,
            self.tasks_created,
            match self.root_create {
                Some(state) => format!("{state:?}"),
                None => "absent".to_owned(),
            },
            self.last_root_poll,
            self.packets,
        )?;
        for (index, (identity, create, state, output_complete)) in self.tasks.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(
                formatter,
                "{identity} {create:?}/{state}/output_complete={output_complete}"
            )?;
        }
        formatter.write_str("]")
    }
}

/// Reads, without changing anything, every fact this attempt's completion
/// still depends on.
fn task_round_wait_facts(
    round: &TaskRound,
    root_task: TaskIdentity,
    packets: usize,
    last_root_poll: RootResultPoll,
    write_completion: Option<&mut WriteCompletionTracker>,
) -> TaskRoundWaitFacts {
    let mut tasks = Vec::new();
    for stage in round.execution().graph().stages() {
        let Some(execution_stage) = round.execution().stage(stage.stage_id()) else {
            continue;
        };
        for (_, task) in execution_stage.tasks() {
            tasks.push((
                task.identity(),
                task.state(),
                task.task_state(),
                task.output_released(),
            ));
        }
    }
    let write = write_completion.map(|tracker| {
        observe_write_statuses(round, tracker);
        tracker.execution_verdict(round.failure_cause().is_some())
    });
    TaskRoundWaitFacts {
        contexts_established: round.contexts_established(),
        tasks_created: round.tasks_created(),
        read: round.execution().read_completion(),
        write,
        root_create: round
            .execution()
            .task(root_task.task_id())
            .map(crate::task_execution::remote_task::RemoteTask::state),
        last_root_poll,
        packets,
        tasks,
    }
}

/// Reports what an attempt is waiting on once it stops changing.
///
/// Driven by the facts rather than by a timer alone: an attempt that is still
/// moving says nothing, and one that has stopped says what it stopped on, once
/// per interval, for as long as it is stopped. One line at the end would not
/// be enough -- the attempt's own timeout is one of the outcomes this has to
/// explain, and a hang that is killed from outside never reaches an end.
struct TaskRoundWaitWitness {
    facts: TaskRoundWaitFacts,
    unchanged_since: Instant,
}

impl TaskRoundWaitWitness {
    const fn new(facts: TaskRoundWaitFacts, now: Instant) -> Self {
        Self {
            facts,
            unchanged_since: now,
        }
    }

    /// Records this turn's facts, reporting them when a whole interval passed
    /// with none of them changing.
    fn observe(&mut self, execution_id: QueryExecutionId, facts: TaskRoundWaitFacts, now: Instant) {
        if self.facts != facts {
            self.facts = facts;
            self.unchanged_since = now;
            return;
        }
        if now.duration_since(self.unchanged_since) < TASK_ROUND_WAIT_REPORT_INTERVAL {
            return;
        }
        self.unchanged_since = now;
        tracing::warn!(
            execution_id = ?execution_id,
            waiting_on = %self.facts,
            "attempt made no observable progress for {:?}; these are the facts its \
             client-visible completion is waiting on",
            TASK_ROUND_WAIT_REPORT_INTERVAL,
        );
    }
}

/// Everything the task-protocol attempt receives from its shared preamble.
///
/// The handoff keeps the inputs move-only: one attempt is scheduled, sealed
/// and budgeted exactly once before its task graph becomes the lifecycle owner.
struct RoundHandoff<'a> {
    query_id: QueryId,
    execution_id: QueryExecutionId,
    statement_deadline: Instant,
    timeout_ms: i64,
    intent: DistributedQueryIntent,
    /// Only the request fields the task round still needs: `artifacts` is consumed
    /// by the preamble that produced `runtime_filter_ready`, so the request
    /// cannot travel whole.
    cancellation: crate::common::query_cancellation::QueryCancellationView,
    completion: crate::query_execution::contract::QueryOutcomeFactory,
    topology: BackendTopologySnapshot,
    statistics_decoder: Option<crate::query_execution::statistics::StatisticsRootResultDecoder>,
    write_decoder: Option<crate::query_execution::write_result::RootWriteResultDecoder>,
    write_stack_session: Option<Arc<crate::query_execution::write_session::ConnectorWriteSession>>,
    backend_services: QueryBackendServices,
    retry_boundary: Option<&'a dyn PreReadyRetryBoundary>,
    runtime_filter_ready: RuntimeFilterDeploymentReadyDistributedQuery,
    init_options: QueryInitOptions,
    feedback_declaration:
        crate::runtime_filter::install_encoder::FrontendRuntimeFilterFeedbackDeclaration,
    feedback_state: Arc<RuntimeFilterFeedbackState>,
    split_assignment_plan: Option<RoundSplitAssignmentPlan>,
    scheduled_backend_ownership: Vec<(usize, BackendProcessId)>,
}

/// Hands the substrate whatever split delivery produced, then steps the runner.
///
/// The order is the reason this is one function: a submission recorded before
/// the turn is released by that same turn, while one recorded after it waits a
/// whole turn for no reason. Every submission taken is reported back, because
/// a submission the substrate refused would otherwise keep its sender blocked
/// until the driver's own timeout and then be resent -- a retry of a decision.
fn advance_task_round(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
) -> Result<TurnReport, TaskExecutionError> {
    for pending in split_delivery.take_pending() {
        let delivery = pending.delivery();
        let task = pending.task();
        let admitted = round
            .execution_mut()
            .enqueue_task_update(task, pending.into_update());
        let reported = match &admitted {
            Ok(admission) => Ok(*admission),
            Err(error) => Err(error),
        };
        split_delivery
            .admit(delivery, reported)
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        // A refused fact is the frontend's own disagreement about what this
        // task may still be told, so it fails the attempt rather than being
        // dropped after the sender has been told about it.
        admitted?;
    }
    round.turn()
}

/// How one failure of an attempt that has not finished starting is judged.
///
/// Before every query context is established, a failure may be the shadow of a
/// backend that was replaced or went unavailable between the schedule and the
/// establish. The failure's own text never decides that; this only records
/// that the membership owner should be asked, and for how long.
///
/// A cancelled attempt asks nobody. The cancellation is held as the live view
/// rather than a snapshot so that question is answered when the failure is
/// judged rather than when this turn's classification was built: the two
/// differ, because one classification judges several break sites.
#[derive(Clone, Copy)]
struct TaskRoundFailureClassification<'a> {
    before_contexts_established: bool,
    captured: &'a BackendTopologySnapshot,
    observation_deadline: Instant,
    cancellation: &'a crate::common::query_cancellation::QueryCancellationView,
}

impl TaskRoundFailureClassification<'_> {
    /// Whether a membership observation could still change this attempt's
    /// answer.
    ///
    /// It cannot once cancellation is latched, and there are only two
    /// consumers of what such an observation would prove. The client is one:
    /// a cancelled statement finishes `Cancelled`, so it is owed
    /// `ER_QUERY_INTERRUPTED` whatever the membership owner goes on to say.
    /// The pre-ready replan is the other, and it must never re-run a
    /// statement the client killed.
    ///
    /// So the observation is pure cost, and on the `KILL QUERY` form it is
    /// cost the killing client pays directly: that interrupt is deliberately
    /// withheld until this worker unwinds, so the client may reuse its
    /// connection (`cancellation_requires_statement_fence` in
    /// `crate::query`). Entering a blocking wait on the membership owner here
    /// is therefore the drain's own rule broken one stage earlier -- the
    /// answer is decided, and every moment spent proving something about it
    /// is a moment the client waits for an answer it could already have had.
    fn observation_can_change_the_answer(self) -> bool {
        !self.cancellation.is_cancelled()
    }
}

/// Whether a pre-ControlReady failure is really this attempt's own, or the
/// shadow of a backend the membership owner can prove was replaced.
///
/// `Some` is typed pre-ready evidence the caller returns as it stands; `None`
/// means the failure belongs to this attempt and the caller fails it closed.
///
/// Split out of `fail_task_round` so the rule is assertable on its own. What
/// it protects is a latency, not a value, and a judgement reachable only
/// through a method that needs a whole live attempt cannot be timed.
fn judge_pre_ready_task_round_failure(
    topology: &dyn BackendTopologyPort,
    classification: TaskRoundFailureClassification<'_>,
    message: &str,
) -> Option<DistributedQueryError> {
    if !classification.before_contexts_established {
        return None;
    }
    // Checked before the observation rather than inside it, because the
    // observation is a blocking wait on the membership owner and the whole
    // point is not to enter one. See
    // `TaskRoundFailureClassification::observation_can_change_the_answer`.
    if !classification.observation_can_change_the_answer() {
        return None;
    }
    let classified = reclassify_pre_ready_lifecycle_failure(
        topology,
        classification.captured,
        DistributedQueryError::pre_ready_topology_observation(message.to_owned()),
        classification.observation_deadline,
    );
    classified
        .pre_ready_topology_outcome()
        .is_some()
        .then_some(classified)
}

/// Whether this task's creation has been acknowledged.
///
/// The root result plane refuses a poll for a task whose creation transaction
/// has not committed, so polling before this is true would turn a normal
/// startup race into a query failure.
fn task_is_created(round: &TaskRound, identity: TaskIdentity) -> bool {
    round
        .execution()
        .task(identity.task_id())
        .is_some_and(|task| matches!(task.state(), RemoteTaskState::Created))
}

/// The wait one root result poll asks for, bounded by the statement deadline.
fn max_root_result_wait(now: Instant, deadline: Instant) -> MaxWait {
    let remaining = deadline.saturating_duration_since(now);
    let wait = remaining
        .min(MAX_ROOT_RESULT_WAIT)
        .max(Duration::from_millis(1));
    // `wait` is already clamped into (0, MAX_ROOT_RESULT_WAIT], which is far
    // inside what `MaxWait` represents, so the fallback is unreachable rather
    // than a silent widening of a wait the deadline had bounded.
    MaxWait::new(wait).unwrap_or_else(|_| MaxWait::default_for(OperationKind::GetFinalTaskInfo))
}

/// One attempt's root result polls, run as an async task beside its owner.
///
/// A poll asks the root task's backend to hold the request until it has
/// something to say, for up to [`MAX_ROOT_RESULT_WAIT`]. The async transport
/// future parks on the process runtime, so it does not occupy the thread that
/// turns the task state machine, dispatches edge opens, settles task
/// acknowledgements, and folds status.
///
/// Every one of those decisions is made by another thread -- a create
/// acknowledgement arriving, a status event published -- so each one that
/// lands while the poll is in flight waits for the poll instead of for its
/// own facts. A distributed `SELECT` opens its producers' edges exactly
/// there, which cost one whole poll wait per edge decision and showed up
/// nowhere except as latency: measured on a 1FE+3BE cluster, every statement
/// of the `filter` suite took ~0.85 s, of which ~0.05 s was work.
///
/// One poll is in flight at a time, in order, and each answer wakes the loop.
/// Data and pending EOS additionally wait for the owner to accept their exact
/// sequence before the following request may acknowledge it.
struct RootResultPolls {
    answers: tokio::sync::mpsc::Receiver<Result<RootResultOutcome, String>>,
    acknowledgements: tokio::sync::mpsc::Sender<ResultPacketSequence>,
    task: tokio::task::JoinHandle<()>,
}

impl RootResultPolls {
    /// Starts polling `root_task`.
    ///
    /// The caller has already observed that this task's creation was
    /// acknowledged: the result plane refuses a poll for a task it does not
    /// hold yet, and that refusal fails the attempt.
    fn start(
        transport: Arc<dyn TaskResultTransport>,
        root_task: TaskIdentity,
        expected_output_schema: novarocks_execution::exec::chunk::ChunkSchemaRef,
        statement_deadline: Instant,
        max_result_bytes: ResultByteLimit,
        wake: Arc<dyn StatusIntakeWake>,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        // Both queues are one item wide because the root stream is ordered.
        // A following fetch cannot carry an ACK until the attempt owner has
        // taken and accepted the preceding packet.
        let (sender, answers) = tokio::sync::mpsc::channel(1);
        let (acknowledgements, mut acknowledged_packets) = tokio::sync::mpsc::channel(1);
        let task = data_runtime.spawn(async move {
            let mut acknowledged = None;
            loop {
                let now = Instant::now();
                if now >= statement_deadline {
                    break;
                }
                let answer = transport
                    .fetch_root_result(
                        root_task,
                        max_root_result_wait(now, statement_deadline),
                        acknowledged,
                        max_result_bytes,
                        Some(Arc::clone(&expected_output_schema)),
                    )
                    .await;
                let expected_acknowledgement = match &answer {
                    Ok(
                        RootResultOutcome::Ready {
                            packet_sequence, ..
                        }
                        | RootResultOutcome::EndOfStreamPending { packet_sequence },
                    ) => Some(ResultPacketSequence::new(*packet_sequence)),
                    _ => None,
                };
                let last = !matches!(
                    &answer,
                    Ok(
                        RootResultOutcome::Ready { .. }
                            | RootResultOutcome::NotReady
                            | RootResultOutcome::EndOfStreamPending { .. }
                    )
                );
                if sender.send(answer).await.is_err() {
                    break;
                }
                wake.wake();
                if let Some(expected) = expected_acknowledgement {
                    match acknowledged_packets.recv().await {
                        Some(actual) if actual == expected => acknowledged = Some(actual),
                        Some(actual) => {
                            let _ = sender
                                .send(Err(format!(
                                    "root result acknowledgement {actual:?} does not match pending {expected:?}"
                                )))
                                .await;
                            wake.wake();
                            break;
                        }
                        None => break,
                    }
                }
                if last {
                    break;
                }
            }
        });
        Ok(Self {
            answers,
            acknowledgements,
            task,
        })
    }

    /// The next answer, if one has arrived.
    ///
    /// A disconnected poller answers `None` for the rest of the attempt: it
    /// stops only after a terminal answer this loop already folded, or at the
    /// statement deadline the loop checks itself.
    fn take(&mut self) -> Option<Result<RootResultOutcome, String>> {
        self.answers.try_recv().ok()
    }

    fn acknowledge(&self, packet_sequence: u64) -> Result<(), String> {
        self.acknowledgements
            .try_send(ResultPacketSequence::new(packet_sequence))
            .map_err(|error| format!("root result acknowledgement could not be queued: {error}"))
    }
}

impl Drop for RootResultPolls {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Feeds every task's latest status to the write completion tracker.
///
/// Every task, not only the declared writers: a task that reported writer
/// facts without being declared a writer means the declared set is not the
/// real one, and that is a fact only the whole set can show.
fn observe_write_statuses(round: &TaskRound, tracker: &mut WriteCompletionTracker) {
    for stage in round.execution().graph().stages() {
        let Some(execution_stage) = round.execution().stage(stage.stage_id()) else {
            continue;
        };
        for (_, task) in execution_stage.tasks() {
            if let Some(status) = task.status() {
                tracker.observe_status(status);
            }
        }
    }
}

/// Reads the bounded final info of every terminal task of this attempt.
///
/// Final info is observation only: success and failure are decided by
/// `TaskStatus` alone, so a task that cannot answer costs diagnostics and
/// nothing else. That is why a failed read is logged and skipped rather than
/// failing a query that already completed.
struct FinalTaskInfoCollector<'a> {
    transport: &'a dyn TaskResultTransport,
    collected: Vec<FinalTaskInfo>,
    read: BTreeSet<TaskId>,
    enabled: bool,
}

impl<'a> FinalTaskInfoCollector<'a> {
    /// Collects nothing unless this attempt's intent asked for a profile.
    ///
    /// Final info is one extra round trip per task, and only `EXPLAIN ANALYZE`
    /// has anything to do with it.
    const fn new(transport: &'a dyn TaskResultTransport, enabled: bool) -> Self {
        Self {
            transport,
            collected: Vec::new(),
            read: BTreeSet::new(),
            enabled,
        }
    }

    /// Reads the final info of every task that has just become terminal.
    ///
    /// Called on each turn rather than once at the end because releasing a
    /// query context is what lets a backend reclaim its retained terminal
    /// records: a sweep after the drain would find exactly the tasks whose
    /// info it wanted already gone.
    ///
    /// Losing one costs diagnostics and nothing else -- success and failure
    /// are decided by `TaskStatus` alone -- so a failed read is reported and
    /// skipped rather than failing a query that already ran.
    fn observe(&mut self, round: &TaskRound) {
        if !self.enabled {
            return;
        }
        for stage in round.execution().graph().stages() {
            let Some(execution_stage) = round.execution().stage(stage.stage_id()) else {
                continue;
            };
            for (task_id, task) in execution_stage.tasks() {
                if !task.is_terminal() || self.read.contains(task_id) {
                    continue;
                }
                let Some(observed) = task.status() else {
                    continue;
                };
                self.read.insert(*task_id);
                match self.transport.final_task_info(task.identity()) {
                    Ok(FinalTaskInfoRead::Available(info)) => {
                        // A final info that contradicts the terminal already
                        // observed is a protocol conflict, not a profile: two
                        // answers would exist to a question with one answer.
                        match accept_final_info(observed, &info) {
                            Ok(()) => self.collected.push(info),
                            Err(error) => tracing::warn!(
                                task = %task.identity(),
                                error = %error,
                                "final task info disagrees with the observed terminal; \
                                 it is dropped from the profile"
                            ),
                        }
                    }
                    Ok(FinalTaskInfoRead::Unavailable(outcome)) => tracing::debug!(
                        task = %task.identity(),
                        outcome = ?outcome,
                        "final task info is unavailable"
                    ),
                    Err(error) => tracing::warn!(
                        task = %task.identity(),
                        error = %error,
                        "final task info could not be read"
                    ),
                }
            }
        }
    }
}

/// Keeps turning until every task terminated and every context was released.
///
/// Bounded by the statement's own deadline. A drain that does not finish
/// inside it is reported and left to the query execution lease, which is the
/// mechanism that exists for exactly this: it never fails a completion that
/// has already been linearized.
/// When a drain gives up, whichever of its own budget and the statement's
/// deadline runs out first.
///
/// Split out from the drain so the rule is assertable: the client-visible
/// answer is already linearized when this is computed, so a drain that
/// silently inherited the statement deadline would hold the caller for the
/// remainder of a long statement timeout with a finished result in hand.
fn drain_deadline(statement_deadline: Instant, drain_budget: Duration, now: Instant) -> Instant {
    statement_deadline.min(now + drain_budget)
}

fn drain_task_round(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
    wake: &CondvarWake,
    split_assignment: Option<&SplitAssignmentRoundGuard>,
    statement_deadline: Instant,
    drain_budget: Duration,
    execution_id: QueryExecutionId,
    final_task_info: &mut FinalTaskInfoCollector<'_>,
) {
    // The drain gets a budget of its own rather than the statement's. The
    // client-visible answer is already linearized, so every moment spent here
    // is a moment the client waits for a result it could already have had: a
    // query that finished in a second against one stuck backend must not hold
    // its caller for the rest of a five-minute statement timeout.
    //
    // The bound is the transport's own queue-residence budget, not a number
    // invented here. That is how long a released operation may sit before the
    // transport itself calls it lost, so a release still unanswered after it
    // is not going to be answered. The statement deadline still caps it: a
    // statement already past its deadline has no time left to lend.
    let deadline = drain_deadline(statement_deadline, drain_budget, Instant::now());
    loop {
        let split_worker_stopped =
            split_assignment.is_none_or(SplitAssignmentRoundGuard::is_finished);
        if round.attempt_drained() && split_worker_stopped {
            return;
        }
        if Instant::now() >= deadline {
            let facts = round.execution().attempt_drain_facts();
            tracing::warn!(
                execution_id = ?execution_id,
                tasks_terminal = facts.all_tasks_terminal(),
                output_released = facts.all_output_released(),
                contexts_released = facts.all_contexts_released(),
                split_worker_stopped,
                "attempt did not finish draining inside its drain budget; \
                 the query execution lease closes what is left"
            );
            return;
        }
        if let Err(error) = advance_task_round(round, split_delivery) {
            tracing::warn!(
                execution_id = ?execution_id,
                error = %error,
                "attempt could not be drained after its client-visible completion"
            );
            return;
        }
        // Each task's final info is read here, while its context still exists:
        // releasing the context is what lets its backend reclaim the retained
        // terminal record this reads.
        final_task_info.observe(round);
        wake.wait(TASK_ROUND_IDLE_WAIT);
    }
}

/// Stands every query context of a failed attempt down.
///
/// Best effort by construction: the attempt has already failed, so an abort
/// that cannot be sent changes nothing about the query's answer -- it only
/// leaves the backends to their lease. What it must not do is return before
/// the aborts have been released to the transport.
fn abort_task_round(round: &mut TaskRound, reason: &str) {
    let contexts = round
        .execution()
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    for context in contexts {
        if let Err(error) = round
            .execution_mut()
            .abort_context(context, AbortCause::QueryFailed)
        {
            tracing::warn!(
                context = %context,
                error = %error,
                reason,
                "query context could not be aborted after the attempt failed"
            );
        }
    }
}
