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
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU16, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use novarocks::query_execution::artifact::{
    NativeSubmissionContext, PreparedNativeExecutionParts, RuntimeFilterDeploymentDispatcher,
    RuntimeFilterDeploymentEpoch, RuntimeFilterDeploymentOptions,
    new_grpc_runtime_filter_deployment_dispatcher,
};
use novarocks::query_execution::backend::LiveBackendTarget;
use novarocks::query_execution::cancellation::QueryCancellationView;
use novarocks::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
    DistributedQueryIntent, DistributedQueryOutcome, DistributedQueryRequest, ProfileReportBuilder,
    QueryId,
};
use novarocks::query_execution::fragment_transport::{
    FetchOutcome, FragmentDispatcher, new_grpc_fragment_dispatcher,
};
use novarocks::query_execution::write::WriteReportBuilder;

use super::backend_events::BackendQueryActivity;
use super::query_registry::FrontendQueryRegistry;
use super::report::FrontendCoordinatorReportHandler;
use super::runtime_filter::FrontendRuntimeFilterDeployment;
use super::scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};

trait QueryIdSource: Send + Sync + 'static {
    fn next_query_id(&self) -> QueryId;
}

struct UniqueQueryIdSource {
    next_low: AtomicI64,
}

impl Default for UniqueQueryIdSource {
    fn default() -> Self {
        Self {
            next_low: AtomicI64::new(100),
        }
    }
}

impl QueryIdSource for UniqueQueryIdSource {
    fn next_query_id(&self) -> QueryId {
        let (high, _) = uuid::Uuid::new_v4().as_u64_pair();
        QueryId::new(
            high as i64,
            self.next_low.fetch_add(1_000, Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
struct FixedQueryIdSource(QueryId);

#[cfg(test)]
impl QueryIdSource for FixedQueryIdSource {
    fn next_query_id(&self) -> QueryId {
        self.0
    }
}

struct FrontendReportEndpointBinding {
    advertised_host: String,
    configured_port: u16,
    bound_port: AtomicU16,
}

impl FrontendReportEndpointBinding {
    fn new(advertised_host: String, configured_port: u16) -> Self {
        Self {
            advertised_host,
            configured_port,
            bound_port: AtomicU16::new(0),
        }
    }

    #[cfg(test)]
    fn from_socket_addr(endpoint: SocketAddr) -> Self {
        Self::new(endpoint.ip().to_string(), endpoint.port())
    }

    fn resolve(
        &self,
    ) -> Result<novarocks::query_execution::backend::CoordinatorReportEndpoint, DistributedQueryError>
    {
        let port = if self.configured_port == 0 {
            let bound = self.bound_port.load(Ordering::Acquire);
            if bound == 0 {
                return Err(failed(
                    "frontend coordinator report endpoint is not bound yet",
                ));
            }
            bound
        } else {
            self.configured_port
        };
        novarocks::query_execution::backend::CoordinatorReportEndpoint::new(
            self.advertised_host.clone(),
            port,
        )
        .map_err(failed)
    }
}

impl novarocks::query_execution::backend::CoordinatorReportEndpointSink
    for FrontendReportEndpointBinding
{
    fn set_bound_port(&self, port: u16) {
        self.bound_port.store(port, Ordering::Release);
    }
}

#[cfg(test)]
enum BackendTransportOverrides {
    Fixed {
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_dispatcher: Arc<dyn RuntimeFilterDeploymentDispatcher>,
    },
}

struct QueryBackendServices {
    scheduler: FrontendFragmentScheduler,
    dispatcher: Arc<dyn FragmentDispatcher>,
    runtime_filter_dispatcher: Arc<dyn RuntimeFilterDeploymentDispatcher>,
}

#[cfg(test)]
impl BackendTransportOverrides {
    fn resolve(
        &self,
        topology: &[LiveBackendTarget],
    ) -> Result<QueryBackendServices, DistributedQueryError> {
        match self {
            #[cfg(test)]
            Self::Fixed {
                dispatcher,
                runtime_filter_dispatcher,
            } => Ok(QueryBackendServices {
                scheduler: FrontendFragmentScheduler::new(
                    FrontendBackendSnapshot::from_live_targets(topology.to_vec())?,
                ),
                dispatcher: Arc::clone(dispatcher),
                runtime_filter_dispatcher: Arc::clone(runtime_filter_dispatcher),
            }),
        }
    }
}

fn production_backend_services(
    topology: &[LiveBackendTarget],
) -> Result<QueryBackendServices, DistributedQueryError> {
    let entries = topology
        .iter()
        .map(|target| (target.backend_idx(), target.endpoint()))
        .collect::<Vec<_>>();
    let snapshot = FrontendBackendSnapshot::from_live_targets(topology.to_vec())?;
    let dispatcher = new_grpc_fragment_dispatcher(&entries).map_err(failed)?;
    let runtime_filter_dispatcher =
        new_grpc_runtime_filter_deployment_dispatcher(&entries).map_err(failed)?;
    Ok(QueryBackendServices {
        scheduler: FrontendFragmentScheduler::new(snapshot),
        dispatcher,
        runtime_filter_dispatcher,
    })
}

pub struct FrontendDistributedQueryCoordinator {
    report_endpoint: Arc<FrontendReportEndpointBinding>,
    backend_topology: novarocks::query_execution::backend::BackendTopologyService,
    #[cfg(test)]
    backend_services: Option<BackendTransportOverrides>,
    runtime_filter_worker_count: NonZeroUsize,
    next_runtime_filter_epoch: AtomicU64,
    #[cfg(test)]
    runtime_filter_barrier_calls: Arc<AtomicU64>,
    query_ids: Arc<dyn QueryIdSource>,
    registry: Arc<FrontendQueryRegistry>,
}

impl FrontendDistributedQueryCoordinator {
    pub fn new(
        advertised_report_host: String,
        configured_report_port: u16,
        runtime_filter_worker_count: NonZeroUsize,
        backend_topology: novarocks::query_execution::backend::BackendTopologyService,
    ) -> Self {
        Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::new(
                advertised_report_host,
                configured_report_port,
            )),
            backend_topology,
            #[cfg(test)]
            backend_services: None,
            runtime_filter_worker_count,
            next_runtime_filter_epoch: AtomicU64::new(1),
            #[cfg(test)]
            runtime_filter_barrier_calls: Arc::new(AtomicU64::new(0)),
            query_ids: Arc::new(UniqueQueryIdSource::default()),
            registry: Arc::new(FrontendQueryRegistry::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        runtime_filter_dispatcher: Arc<dyn RuntimeFilterDeploymentDispatcher>,
    ) -> Self {
        Self::new_for_test_with_topology(
            query_id,
            report_endpoint,
            scheduler.clone(),
            dispatcher,
            runtime_filter_worker_count,
            runtime_filter_dispatcher,
            Arc::new(
                crate::topology::ClusterBackendService::from_captured_targets_for_test(
                    &scheduler
                        .backend_entries()
                        .iter()
                        .map(|(backend_idx, endpoint)| {
                            novarocks::query_execution::backend::LiveBackendTarget::new(
                                *backend_idx,
                                *endpoint,
                                0,
                            )
                        })
                        .collect::<Vec<_>>(),
                ),
            ),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_with_topology(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        _scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        runtime_filter_dispatcher: Arc<dyn RuntimeFilterDeploymentDispatcher>,
        backend_topology: novarocks::query_execution::backend::BackendTopologyService,
    ) -> Self {
        Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::from_socket_addr(
                report_endpoint,
            )),
            backend_topology,
            backend_services: Some(BackendTransportOverrides::Fixed {
                dispatcher,
                runtime_filter_dispatcher,
            }),
            runtime_filter_worker_count,
            next_runtime_filter_epoch: AtomicU64::new(1),
            runtime_filter_barrier_calls: Arc::new(AtomicU64::new(0)),
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::default()),
        }
    }

    pub fn report_handler(&self) -> FrontendCoordinatorReportHandler {
        FrontendCoordinatorReportHandler::new(Arc::clone(&self.registry))
    }

    pub fn backend_query_activity(&self) -> BackendQueryActivity {
        BackendQueryActivity::new(Arc::clone(&self.registry))
    }

    pub fn report_endpoint_sink(
        &self,
    ) -> Arc<dyn novarocks::query_execution::backend::CoordinatorReportEndpointSink> {
        self.report_endpoint.clone()
    }

    #[cfg(test)]
    pub(crate) fn runtime_filter_barrier_calls(&self) -> u64 {
        self.runtime_filter_barrier_calls.load(Ordering::SeqCst)
    }

    pub fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }

    fn next_runtime_filter_epoch(
        &self,
    ) -> Result<RuntimeFilterDeploymentEpoch, DistributedQueryError> {
        let epoch = self
            .next_runtime_filter_epoch
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::Rejected,
                    "frontend runtime-filter deployment epoch space is exhausted",
                )
            })?;
        RuntimeFilterDeploymentEpoch::new(epoch)
    }

    fn execute_request(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let query_id = self.query_ids.next_query_id();
        let parts = request.into_parts();
        let intent = parts.completion.intent();
        self.backend_topology
            .validate_snapshot(&parts.topology)
            .map_err(|error| failed(error.to_string()))?;
        #[cfg(test)]
        let backend_services = match &self.backend_services {
            Some(services) => services.resolve(parts.topology.targets())?,
            None => production_backend_services(parts.topology.targets())?,
        };
        #[cfg(not(test))]
        let backend_services = production_backend_services(parts.topology.targets())?;
        let dispatcher = backend_services.dispatcher;
        let _query = self
            .registry
            .register(query_id, intent, Arc::clone(&dispatcher))?;
        let schedule = backend_services
            .scheduler
            .schedule(parts.artifacts.scheduling_view(), query_id)?;
        let scheduled_backend_ownership = backend_services
            .scheduler
            .scheduled_backend_ownership(&schedule.backend_ids())?;
        self.backend_topology
            .validate_snapshot(&parts.topology)
            .map_err(|error| failed(error.to_string()))?;
        self.registry
            .set_scheduled_backend_ownership(query_id, &scheduled_backend_ownership)?;
        let scheduled = parts.artifacts.bind_schedule(schedule)?;
        #[cfg(test)]
        let runtime_filters = FrontendRuntimeFilterDeployment::with_barrier_counter(
            backend_services.runtime_filter_dispatcher,
            Arc::clone(&self.runtime_filter_barrier_calls),
        );
        #[cfg(not(test))]
        let runtime_filters =
            FrontendRuntimeFilterDeployment::new(backend_services.runtime_filter_dispatcher);
        let runtime_filter_options = RuntimeFilterDeploymentOptions::new(
            self.next_runtime_filter_epoch()?,
            backend_services.scheduler.backend_entries().to_vec(),
            self.runtime_filter_worker_count.get(),
            parts.options.runtime_filter_lifecycle(),
        )?;
        let context = NativeSubmissionContext::new(
            query_id,
            &parts.options,
            self.report_endpoint.resolve()?,
            dispatcher.needs_fragment_status_report() || intent == DistributedQueryIntent::Profile,
        );
        let ready = scheduled.prepare_runtime_filters(runtime_filter_options, &runtime_filters)?;
        let execution = ready.assemble(context)?;
        let PreparedNativeExecutionParts {
            submissions,
            root_fetch,
            writer_registrations,
            expected_output,
            runtime_filter_lease,
        } = execution.into_parts();
        let mut runtime_filter_lease = Some(runtime_filter_lease);
        let submitted_instance_ids = submissions
            .iter()
            .map(|submission| submission.fragment_instance_id())
            .collect::<Vec<_>>();
        let writer_instance_ids = writer_registrations.fragment_instance_ids();
        let writer_identities = writer_registrations.writer_identities();
        if let Err(error) = self
            .registry
            .set_writer_instances(query_id, &writer_identities)
        {
            let kind = error.kind();
            let message =
                abort_runtime_filters(&mut runtime_filter_lease, error.message().to_string());
            return Err(DistributedQueryError::new(kind, message));
        }
        let deadline = parts.deadline;

        let submission_count = submissions.len();
        let mut submitted = 0usize;
        for submission in submissions {
            if parts.cancellation.is_cancelled() {
                let error = self.fail_cancel_then_abort_runtime_filters(
                    query_id,
                    &mut runtime_filter_lease,
                    "query cancelled before fragment submission",
                );
                if intent == DistributedQueryIntent::Write {
                    break;
                }
                return Err(error);
            }
            let backend_idx = submission.backend_idx();
            let finst_id = submission.fragment_instance_id();
            if let Err(error) = self
                .registry
                .record_attempt(query_id, backend_idx, finst_id)
            {
                let message = abort_runtime_filters(&mut runtime_filter_lease, error.to_string());
                let _ = self
                    .registry
                    .preserve_failure_context(query_id, message.clone());
                if intent == DistributedQueryIntent::Write {
                    break;
                }
                return Err(failed(message));
            }
            let submit_result = dispatcher.submit_fragment(backend_idx, submission.into_envelope());
            if let Err(error) = self.registry.finish_attempt(query_id) {
                let message = abort_runtime_filters(&mut runtime_filter_lease, error.to_string());
                let _ = self
                    .registry
                    .preserve_failure_context(query_id, message.clone());
                if intent == DistributedQueryIntent::Write {
                    break;
                }
                return Err(failed(message));
            }
            if let Err(error) = submit_result {
                let error = self.fail_cancel_then_abort_runtime_filters(
                    query_id,
                    &mut runtime_filter_lease,
                    error,
                );
                if intent == DistributedQueryIntent::Write {
                    break;
                }
                return Err(error);
            }
            self.backend_topology
                .record_successful_fragment_submission(backend_idx);
            submitted += 1;
            if let Some(message) = self.registry.first_failure(query_id) {
                if intent != DistributedQueryIntent::Write {
                    let message = abort_runtime_filters(&mut runtime_filter_lease, message);
                    return Err(failed(message));
                }
                break;
            }
        }
        let submission_failure = self.registry.first_failure(query_id);
        if submitted == submission_count
            && submission_failure.is_none()
            && !parts.cancellation.is_cancelled()
        {
            runtime_filter_lease
                .take()
                .expect("runtime-filter lease is present through fragment submission")
                .release();
        } else {
            let message = submission_failure.unwrap_or_else(|| {
                if parts.cancellation.is_cancelled() {
                    "query cancelled during fragment submission".to_string()
                } else {
                    "fragment submission stopped before completion".to_string()
                }
            });
            if self.registry.first_failure(query_id).is_some() {
                let message = abort_runtime_filters(&mut runtime_filter_lease, message);
                let _ = self.registry.preserve_failure_context(query_id, message);
            } else {
                let _ = self.fail_cancel_then_abort_runtime_filters(
                    query_id,
                    &mut runtime_filter_lease,
                    message,
                );
            }
        }

        if parts.cancellation.is_cancelled() {
            let error = self.fail_and_cancel(query_id, "query cancelled after fragment submission");
            if intent != DistributedQueryIntent::Write {
                return Err(error);
            }
        }
        if let Some(message) = self.registry.first_failure(query_id)
            && intent != DistributedQueryIntent::Write
        {
            return Err(failed(message));
        }

        let mut batches = Vec::new();
        if root_fetch.uses_result_buffer() {
            loop {
                if parts.cancellation.is_cancelled() {
                    return Err(
                        self.fail_and_cancel(query_id, "query cancelled while fetching result")
                    );
                }
                if let Some(message) = self.registry.first_failure(query_id) {
                    return Err(failed(message));
                }
                let now = Instant::now();
                let fetch_wait_ms = match deadline {
                    Some(deadline) if now >= deadline => {
                        return Err(self.fail_and_cancel(query_id, "query deadline exceeded"));
                    }
                    Some(deadline) => deadline
                        .saturating_duration_since(now)
                        .as_millis()
                        .clamp(1, 300) as i64,
                    None => 300,
                };
                let fetch = match dispatcher.fetch_result(
                    root_fetch.backend_idx(),
                    root_fetch.fragment_instance_id(),
                    fetch_wait_ms,
                    Some(expected_output.fetch_view()),
                ) {
                    Ok(fetch) => fetch,
                    Err(error) => return Err(self.fail_and_cancel(query_id, error)),
                };
                match fetch {
                    FetchOutcome::Ready(batch) => batches.push(batch),
                    FetchOutcome::NotReady => continue,
                    FetchOutcome::Eof => break,
                    FetchOutcome::Err(error) => {
                        return Err(self.fail_and_cancel(query_id, error));
                    }
                }
            }
        }

        match intent {
            DistributedQueryIntent::Result => {}
            DistributedQueryIntent::Write => {
                if let Err(error) = self.wait_for_reports(
                    query_id,
                    &writer_instance_ids,
                    deadline,
                    &parts.cancellation,
                    true,
                    "write final reports",
                ) {
                    if self.registry.first_failure(query_id).is_none() {
                        let _ = self
                            .registry
                            .latch_failure_and_cancel(query_id, error.message().to_string());
                    }
                }
            }
            DistributedQueryIntent::Profile => self.wait_for_reports(
                query_id,
                &submitted_instance_ids,
                deadline,
                &parts.cancellation,
                false,
                "fragment profile reports",
            )?,
        }

        let (query_failure, reports) = self.registry.seal_and_take_completion(query_id)?;
        let outcome = (|| {
            let result = expected_output.into_query_result(batches)?;
            match intent {
                DistributedQueryIntent::Result => parts.completion.result(result),
                DistributedQueryIntent::Write => {
                    let mut builder = WriteReportBuilder::new(writer_registrations)?;
                    if let Some(message) = query_failure {
                        builder.latch_failure(message);
                    }
                    for report in reports {
                        builder.apply(report)?;
                    }
                    let report_outcome = builder.finish()?;
                    if let Some(reason) = report_outcome.abort_reason() {
                        let _ = self
                            .registry
                            .latch_failure_and_cancel(query_id, reason.to_string());
                    }
                    let (commit, abort) = report_outcome.into_payloads();
                    parts.completion.write(result, commit, abort)
                }
                DistributedQueryIntent::Profile => {
                    let mut builder = ProfileReportBuilder::new();
                    for report in reports {
                        builder.apply(report)?;
                    }
                    parts.completion.profile(result, builder.finish())
                }
            }
        })();
        if let Err(error) = &outcome {
            let _ = self
                .registry
                .latch_failure_and_cancel(query_id, error.message().to_string());
        }
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    fn wait_for_reports(
        &self,
        query_id: QueryId,
        expected_instances: &[novarocks::UniqueId],
        deadline: Option<Instant>,
        cancellation: &QueryCancellationView,
        final_report_failure_completes_wait: bool,
        report_kind: &str,
    ) -> Result<(), DistributedQueryError> {
        const REPORT_POLL_INTERVAL: Duration = Duration::from_millis(10);

        if expected_instances.is_empty() {
            return Ok(());
        }

        loop {
            let (received, first_failure, has_failed_final_report) = self
                .registry
                .report_progress(query_id, expected_instances)?;
            if let Some(message) = first_failure {
                if final_report_failure_completes_wait && has_failed_final_report {
                    return Ok(());
                }
                return Err(self.fail_and_cancel(query_id, message));
            }
            if received >= expected_instances.len() {
                return Ok(());
            }
            if cancellation.is_cancelled() {
                return Err(self.fail_and_cancel(
                    query_id,
                    format!("query cancelled while waiting for {report_kind}"),
                ));
            }

            let now = Instant::now();
            match deadline {
                Some(deadline) if now >= deadline => {
                    return Err(self.fail_and_cancel(
                        query_id,
                        format!("query deadline exceeded waiting for {report_kind}: received {received} of {}", expected_instances.len()),
                    ));
                }
                Some(deadline) => std::thread::sleep(
                    deadline
                        .saturating_duration_since(now)
                        .min(REPORT_POLL_INTERVAL),
                ),
                None => std::thread::sleep(REPORT_POLL_INTERVAL),
            }
        }
    }

    fn fail_and_cancel(
        &self,
        query_id: QueryId,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        match self.registry.latch_failure_and_cancel(query_id, message) {
            Ok(message) => failed(message),
            Err(error) => error,
        }
    }

    fn fail_cancel_then_abort_runtime_filters(
        &self,
        query_id: QueryId,
        lease: &mut Option<novarocks::query_execution::artifact::RuntimeFilterInstallLease>,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        let primary = self.fail_and_cancel(query_id, message);
        let enriched = abort_runtime_filters(lease, primary.message().to_string());
        let _ = self
            .registry
            .preserve_failure_context(query_id, enriched.clone());
        failed(self.registry.first_failure(query_id).unwrap_or(enriched))
    }
}

impl DistributedQueryCoordinator for FrontendDistributedQueryCoordinator {
    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

fn abort_runtime_filters(
    lease: &mut Option<novarocks::query_execution::artifact::RuntimeFilterInstallLease>,
    message: impl Into<String>,
) -> String {
    let message = message.into();
    lease
        .take()
        .map_or(message.clone(), |lease| lease.abort_preserving(message))
}

#[cfg(test)]
mod tests {
    use super::FrontendReportEndpointBinding;
    use novarocks::query_execution::backend::CoordinatorReportEndpointSink;

    #[test]
    fn ephemeral_report_endpoint_is_unavailable_until_the_bound_port_is_published() {
        let binding = FrontendReportEndpointBinding::new("frontend.internal".to_string(), 0);

        let error = binding
            .resolve()
            .err()
            .expect("port zero must gate query submission until listener bind");
        assert!(error.message().contains("not bound yet"), "{error}");

        binding.set_bound_port(19070);

        binding
            .resolve()
            .expect("bound port publication makes the DNS endpoint available");
    }
}
