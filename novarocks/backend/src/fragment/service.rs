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

use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

use novarocks::common::app_config;
use novarocks::novarocks_logging::{error, info, warn};
use novarocks::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentEventSink, FragmentLookupClient, FragmentResultWriter,
};
use novarocks::runtime::fragment::{
    FragmentCancelReason, FragmentOutcome, RunningFragmentHandle, prepare_fragment,
};
use novarocks::runtime::native_fragment_query::NativeFragmentQueryRuntime;
use novarocks::runtime::profile::Profiler;
use novarocks::service::fe_report;
use novarocks::service::native_fragment_ingress::{
    NativeFragmentAccepted, NativeFragmentCancelRequest, NativeFragmentIngress,
    NativeFragmentIngressError, NativeFragmentRequest,
};

use super::control::{FragmentControlHandle, FragmentControlRegistry};
use super::failure_injection::start_with_configured_fragment_failure_trigger;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeFragmentLifecycleEvent {
    Prepared,
    Registered,
    Accepted,
    Started,
}

type LifecycleObserver = Arc<dyn Fn(NativeFragmentLifecycleEvent) + Send + Sync>;

pub struct NativeFragmentService {
    pub(super) controls: Arc<FragmentControlRegistry>,
    queries: NativeFragmentQueryRuntime,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    lookup_client: Arc<dyn FragmentLookupClient>,
    result_writer: Arc<dyn FragmentResultWriter>,
    event_sink: Arc<dyn FragmentEventSink>,
    lifecycle_observer: Option<LifecycleObserver>,
    #[cfg(test)]
    fail_worker_spawn_on_submission: Option<usize>,
    #[cfg(test)]
    submission_count: AtomicUsize,
}

impl std::fmt::Debug for NativeFragmentService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeFragmentService")
            .finish_non_exhaustive()
    }
}

impl NativeFragmentService {
    pub fn new(
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self {
            controls: Arc::new(FragmentControlRegistry::default()),
            queries: NativeFragmentQueryRuntime::global(),
            exchange_transmitter,
            lookup_client,
            result_writer,
            event_sink,
            lifecycle_observer: None,
            #[cfg(test)]
            fail_worker_spawn_on_submission: None,
            #[cfg(test)]
            submission_count: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    fn with_lifecycle_observer(
        observer: impl Fn(NativeFragmentLifecycleEvent) + Send + Sync + 'static,
    ) -> Self {
        Self {
            lifecycle_observer: Some(Arc::new(observer)),
            ..Self::new(
                crate::fragment::grpc_exchange_transmitter(),
                crate::fragment::grpc_fragment_lookup_client(),
                crate::fragment::native_result_writer(),
                crate::fragment::native_fragment_event_sink(),
            )
        }
    }

    #[cfg(test)]
    fn with_lifecycle_observer_and_worker_spawn_failure(
        observer: impl Fn(NativeFragmentLifecycleEvent) + Send + Sync + 'static,
        fail_worker_spawn_on_submission: usize,
    ) -> Self {
        Self {
            lifecycle_observer: Some(Arc::new(observer)),
            fail_worker_spawn_on_submission: Some(fail_worker_spawn_on_submission),
            ..Self::new(
                crate::fragment::grpc_exchange_transmitter(),
                crate::fragment::grpc_fragment_lookup_client(),
                crate::fragment::native_result_writer(),
                crate::fragment::native_fragment_event_sink(),
            )
        }
    }

    fn observe(&self, event: NativeFragmentLifecycleEvent) {
        if let Some(observer) = self.lifecycle_observer.as_ref() {
            observer(event);
        }
    }
}

impl NativeFragmentIngress for NativeFragmentService {
    fn submit(
        &self,
        request: NativeFragmentRequest,
    ) -> Result<NativeFragmentAccepted, NativeFragmentIngressError> {
        let query_id = request.query_id();
        let fragment_instance_id = request.fragment_instance_id();
        let backend_num = request.backend_num();
        let report_endpoint = request.report_endpoint().cloned();
        let enable_profile = request.enable_profile();
        let report_interval_ns = profile_report_interval_ns(
            enable_profile,
            request.runtime_profile_report_interval_seconds(),
        );
        let (delivery_expire, query_expire) = request.query_expire_durations();
        let cache_options = request.cache_options()?;
        let profiler =
            enable_profile.then(|| profiler_for_native_fragment(request.root_plan_node_id()));
        let admission = self
            .queries
            .prepare_admission(
                query_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
                cache_options,
                request.has_runtime_filter_bindings(),
            )
            .map_err(NativeFragmentIngressError::new)?;
        let query_mem_tracker = admission.query_mem_tracker();
        let fragment_mem_tracker = admission.fragment_mem_tracker();
        let failure_injection_eligible = !request.uses_result_sink();
        let dormant = prepare_fragment(
            request.into_submission(),
            admission.into_prepare_context(
                profiler.clone(),
                Arc::clone(&self.exchange_transmitter),
                Arc::clone(&self.lookup_client),
                Arc::clone(&self.result_writer),
                Arc::clone(&self.event_sink),
            ),
        )
        .map_err(NativeFragmentIngressError::new)?;
        self.observe(NativeFragmentLifecycleEvent::Prepared);

        let reservation = self
            .controls
            .reserve(fragment_instance_id)
            .map_err(NativeFragmentIngressError::new)?;
        let registration = self
            .queries
            .register_fragment(
                query_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
            )
            .map_err(NativeFragmentIngressError::new)?;

        let (start_tx, start_rx) = mpsc::sync_channel::<()>(0);
        let queries = self.queries.clone();
        let observer = self.lifecycle_observer.clone();
        #[cfg(test)]
        if self.fail_worker_spawn_on_submission.is_some_and(|target| {
            self.submission_count.fetch_add(1, Ordering::SeqCst) + 1 == target
        }) {
            return Err(NativeFragmentIngressError::new(
                "injected native fragment adapter worker spawn failure",
            ));
        }
        std::thread::Builder::new()
            .name(format!(
                "native-fragment-{:x}-{:x}",
                fragment_instance_id.hi, fragment_instance_id.lo
            ))
            .spawn(move || {
                if start_rx.recv().is_err() {
                    let error = "native fragment start signal was dropped".to_string();
                    fe_report::report_fragment_done(fragment_instance_id, Some(error), false);
                    return;
                }
                let (running, failure_release) =
                    start_with_configured_fragment_failure_trigger(
                        dormant,
                        failure_injection_eligible,
                    );
                let control = Arc::new(RunningFragmentControl {
                    handle: running.clone(),
                });
                let token = reservation.publish(control);
                registration.into_running();
                if let Some(observer) = observer.as_ref() {
                    observer(NativeFragmentLifecycleEvent::Started);
                }
                if let Some(release) = failure_release {
                    match release.wait() {
                        Ok(evidence_token) => {
                            eprintln!(
                                "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token={} query_hi={} query_lo={} finst_hi={} finst_lo={}",
                                evidence_token,
                                query_id.hi(),
                                query_id.lo(),
                                fragment_instance_id.hi,
                                fragment_instance_id.lo
                            );
                        }
                        Err(error) => {
                            eprintln!(
                                "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_RELEASE_FAILED query_hi={} query_lo={} finst_hi={} finst_lo={} error={}",
                                query_id.hi(),
                                query_id.lo(),
                                fragment_instance_id.hi,
                                fragment_instance_id.lo,
                                error
                            );
                        }
                    }
                }
                consume_terminal_fact(running, token, queries);
            })
            .map_err(|error| {
                NativeFragmentIngressError::new(format!(
                    "spawn native fragment adapter worker failed: {error}"
                ))
            })?;

        if let Some(report_endpoint) = report_endpoint {
            fe_report::register_novarocks_instance(
                fragment_instance_id,
                query_id,
                report_endpoint,
                backend_num,
                enable_profile,
                profiler,
                Some(fragment_mem_tracker),
                Some(query_mem_tracker),
                report_interval_ns,
            );
        } else {
            warn!(
                target: "novarocks::report",
                finst_id = %fragment_instance_id,
                "missing native report_endpoint for reportExecStatus"
            );
        }
        self.observe(NativeFragmentLifecycleEvent::Registered);
        self.observe(NativeFragmentLifecycleEvent::Accepted);
        start_tx.send(()).map_err(|_| {
            NativeFragmentIngressError::new(
                "native fragment adapter worker terminated before start",
            )
        })?;
        Ok(NativeFragmentAccepted::new(query_id, fragment_instance_id))
    }

    fn cancel(
        &self,
        request: NativeFragmentCancelRequest,
    ) -> Result<(), NativeFragmentIngressError> {
        // Design: ADR-0010 (docs/adr/ADR-0010-explicit-query-cancellation-surface.md)
        let mut fragment_instance_ids = request
            .fragment_instance_ids()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        fragment_instance_ids.extend(
            self.queries
                .cancel_query(request.query_id(), request.reason().to_string()),
        );
        let fragment_instance_ids = fragment_instance_ids.into_iter().collect::<Vec<_>>();
        self.controls
            .cancel_many(&fragment_instance_ids, request.reason());
        Ok(())
    }
}

struct RunningFragmentControl {
    handle: RunningFragmentHandle,
}

impl FragmentControlHandle for RunningFragmentControl {
    fn cancel(&self, reason: &str) {
        self.handle.cancel(FragmentCancelReason::new(reason));
    }
}

fn consume_terminal_fact(
    running: RunningFragmentHandle,
    token: super::control::FragmentControlToken,
    queries: NativeFragmentQueryRuntime,
) {
    let fact = running.join();
    let query_id = fact.query_id();
    let fragment_instance_id = fact.fragment_instance_id();
    let report_error = match fact.outcome() {
        FragmentOutcome::Succeeded => {
            if let Some(profile) = fact.profile() {
                info!(
                    target: "novarocks::profile",
                    finst_id = %fragment_instance_id,
                    profile = ?profile,
                    "native_fragment_profile"
                );
            }
            None
        }
        FragmentOutcome::Failed(execution_error) => {
            let report_error = execution_error.to_string();
            error!(
                target: "novarocks::exec",
                finst_id = %fragment_instance_id,
                error = %execution_error,
                "native fragment execution failed"
            );
            Some(report_error)
        }
        FragmentOutcome::Cancelled { reason } => Some(reason.detail().to_string()),
    };
    let report_decision = queries.finish_fragment_for_report(query_id);
    fe_report::report_fragment_done(
        fragment_instance_id,
        report_error,
        report_decision.include_runtime_filter_profile(),
    );
    queries.unregister_fragment(fragment_instance_id);
    queries.cleanup_after_fragment_report(query_id, report_decision);
    token.complete();
}

fn profiler_for_native_fragment(root_plan_node_id: i32) -> Profiler {
    let profiler = Profiler::new(format!(
        "execute_fragment_native (plan_node_id={root_plan_node_id})"
    ));
    profiler.set_metadata(i64::from(root_plan_node_id));
    profiler
}

fn profile_report_interval_ns(
    enable_profile: bool,
    query_interval_seconds: Option<i64>,
) -> Option<i64> {
    if !enable_profile {
        return None;
    }
    query_interval_seconds
        .filter(|value| *value > 0)
        .and_then(|value| value.checked_mul(1_000_000_000))
        .or_else(|| {
            app_config::config()
                .ok()
                .map(|config| config.runtime.profile_report_interval.max(1) * 1_000_000_000)
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use novarocks::UniqueId;
    use novarocks::proto;
    use novarocks::runtime::fragment::{FragmentOutcome, FragmentPrepareContext, prepare_fragment};
    use novarocks::runtime::query_context::QueryId;
    use novarocks::service::native_fragment_ingress::{
        NativeFragmentCancelRequest, NativeFragmentIngress, NativeFragmentRequest,
    };

    use crate::fragment::control::FragmentControlHandle;
    use crate::fragment::failure_injection::{
        FRAGMENT_EXECUTOR_FAILURE_MESSAGE, start_with_fragment_failure_trigger,
    };

    use super::{
        NativeFragmentLifecycleEvent, NativeFragmentService, RunningFragmentControl,
        consume_terminal_fact,
    };

    #[derive(Default)]
    struct RecordingControl {
        reasons: Mutex<Vec<String>>,
    }

    impl FragmentControlHandle for RecordingControl {
        fn cancel(&self, reason: &str) {
            self.reasons
                .lock()
                .expect("recording control reasons")
                .push(reason.to_string());
        }
    }

    #[test]
    fn cancel_latches_query_context_before_cancelling_all_local_controls() {
        let service = NativeFragmentService::with_lifecycle_observer(|_| {});
        let query_id = QueryId::new(84_000, 84_001);
        let requested = UniqueId {
            hi: 84_002,
            lo: 84_003,
        };
        let local_sibling = UniqueId {
            hi: 84_004,
            lo: 84_005,
        };
        let mut controls = Vec::new();
        let mut control_tokens = Vec::new();
        for finst in [requested, local_sibling] {
            let registration = service
                .queries
                .register_fragment(
                    query_id,
                    finst,
                    Duration::from_secs(1),
                    Duration::from_secs(5),
                )
                .expect("register local fragment");
            registration.into_running();
            let control = Arc::new(RecordingControl::default());
            let control_handle: Arc<dyn FragmentControlHandle> = control.clone();
            let token = service
                .controls
                .reserve(finst)
                .expect("reserve local control")
                .publish(control_handle);
            controls.push(control);
            control_tokens.push(token);
        }

        service
            .cancel(NativeFragmentCancelRequest::new(
                query_id,
                vec![requested],
                "explicit query cancellation",
            ))
            .expect("cancel is idempotent");
        assert!(controls.iter().all(|control| {
            control
                .reasons
                .lock()
                .expect("recording control reasons")
                .iter()
                .any(|reason| reason == "explicit query cancellation")
        }));
        assert!(
            !service
                .queries
                .cancel_query(query_id, "repeat probe".to_string())
                .is_empty(),
            "canonical query cancellation retains the local fragment set until terminal cleanup"
        );
        service
            .cancel(NativeFragmentCancelRequest::new(
                query_id,
                vec![requested],
                "explicit query cancellation",
            ))
            .expect("repeat cancel is idempotent");
        drop(control_tokens);
    }

    fn values_result_request(query_base: i64, fragment_base: i64) -> NativeFragmentRequest {
        let fragment_id = 7;
        NativeFragmentRequest::try_decode(
            proto::plan::PlanFragment {
                fragment_id,
                root: Some(proto::plan::DistributedNode {
                    node_id: 41,
                    fragment_id,
                    limit: -1,
                    payload: Some(proto::plan::distributed_node::Payload::Physical(
                        proto::plan::PlanNode {
                            output_columns: Vec::new(),
                            kind: Some(proto::plan::plan_node::Kind::Values(
                                proto::plan::ValuesNode {
                                    rows: Vec::new(),
                                    columns: Vec::new(),
                                },
                            )),
                        },
                    )),
                    ..Default::default()
                }),
                sink: Some(proto::plan::DataSink {
                    kind: Some(proto::plan::data_sink::Kind::Result(true)),
                }),
                output_columns: Vec::new(),
                runtime_filter_bindings: Some(proto::plan::RuntimeFilterBindingTable {
                    fragment_id,
                    bindings: Vec::new(),
                }),
                ..Default::default()
            },
            proto::novarocks::InstanceParams {
                query_id: Some(proto::common::UniqueId {
                    hi: query_base,
                    lo: query_base + 1,
                }),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: fragment_base,
                    lo: fragment_base + 1,
                }),
                backend_num: 3,
                query_options: Some(proto::novarocks::QueryOptions {
                    batch_size: 1024,
                    pipeline_dop: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .expect("valid native fragment request")
    }

    #[test]
    fn submit_acceptance_point_follows_prepare_and_registration_before_start() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let service = NativeFragmentService::with_lifecycle_observer(move |event| {
            captured.lock().expect("lifecycle events").push(event);
        });

        service
            .submit(values_result_request(81_000, 81_002))
            .expect("native fragment submit");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while events.lock().expect("lifecycle events").len() < 4
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert_eq!(
            *events.lock().expect("lifecycle events"),
            vec![
                NativeFragmentLifecycleEvent::Prepared,
                NativeFragmentLifecycleEvent::Registered,
                NativeFragmentLifecycleEvent::Accepted,
                NativeFragmentLifecycleEvent::Started,
            ]
        );
    }

    #[test]
    fn registration_failure_drops_dormant_resources_before_retry() {
        let service = NativeFragmentService::new(
            crate::fragment::grpc_exchange_transmitter(),
            crate::fragment::grpc_fragment_lookup_client(),
            crate::fragment::native_result_writer(),
            crate::fragment::native_fragment_event_sink(),
        );
        let first = values_result_request(82_000, 82_002);
        let finst_id = first.fragment_instance_id();
        let reservation = service
            .controls
            .reserve(finst_id)
            .expect("reserve conflicting service route");

        let error = service
            .submit(first)
            .expect_err("duplicate service registration must fail");
        assert!(error.to_string().contains("already registered"), "{error}");

        drop(reservation);
        service
            .submit(values_result_request(82_000, 82_002))
            .expect("retry must observe rolled-back dormant resources");
    }

    #[test]
    fn second_worker_spawn_failure_rolls_back_only_its_pre_start_registration() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let worker_release = Arc::clone(&release_rx);
        let service = NativeFragmentService::with_lifecycle_observer_and_worker_spawn_failure(
            move |event| {
                if event == NativeFragmentLifecycleEvent::Started {
                    started_tx.send(()).expect("publish first worker start");
                    worker_release
                        .lock()
                        .expect("first worker release")
                        .recv()
                        .expect("release first worker");
                }
            },
            2,
        );
        let query_id = QueryId::new(83_000, 83_001);
        let first = UniqueId {
            hi: 83_002,
            lo: 83_003,
        };
        let second = UniqueId {
            hi: 83_004,
            lo: 83_005,
        };

        service
            .submit(values_result_request(83_000, 83_002))
            .expect("first fragment reaches running");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first worker remains registered");

        let error = service
            .submit(values_result_request(83_000, 83_004))
            .expect_err("second worker spawn is injected to fail");
        assert!(error.to_string().contains("spawn failure"), "{error}");
        assert!(
            service.controls.reserve(first).is_err(),
            "first running route must remain registered"
        );
        drop(
            service
                .controls
                .reserve(second)
                .expect("failed second registration must release its route"),
        );

        release_tx.send(()).expect("release first worker");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match service.controls.reserve(first) {
                Ok(reservation) => {
                    drop(reservation);
                    break;
                }
                Err(_) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("first fragment did not terminate: {error}"),
            }
        }
        assert!(
            service
                .queries
                .cancel_query(query_id, "post-terminal probe".to_string())
                .is_empty(),
            "terminated query must not retain either fragment mapping"
        );
    }

    #[test]
    fn native_failed_terminal_fact_does_not_locally_cancel_siblings_before_frontend_ack() {
        let service = NativeFragmentService::new();
        let request = values_result_request(83_100, 83_104);
        let query_id = request.query_id();
        let failed_finst = request.fragment_instance_id();
        let sibling_finst = UniqueId {
            hi: 83_102,
            lo: 83_103,
        };
        let delivery_expire = Duration::from_secs(1);
        let query_expire = Duration::from_secs(5);

        let sibling_registration = service
            .queries
            .register_fragment(query_id, sibling_finst, delivery_expire, query_expire)
            .expect("register sibling fragment");
        sibling_registration.into_running();
        let sibling_control = Arc::new(RecordingControl::default());
        let sibling_token = service
            .controls
            .reserve(sibling_finst)
            .expect("reserve sibling control")
            .publish(sibling_control.clone());

        let failed_registration = service
            .queries
            .register_fragment(query_id, failed_finst, delivery_expire, query_expire)
            .expect("register failed fragment");
        failed_registration.into_running();
        let failed = prepare_fragment(request.into_submission(), FragmentPrepareContext::default())
            .expect("failed fragment prepares")
            .start_failed("native executor failure");
        let failed_token = service
            .controls
            .reserve(failed_finst)
            .expect("reserve failed control")
            .publish(Arc::new(RunningFragmentControl {
                handle: failed.clone(),
            }));

        consume_terminal_fact(failed, failed_token, service.queries.clone());

        assert!(
            sibling_control
                .reasons
                .lock()
                .expect("recording control reasons")
                .is_empty(),
            "native failure must be reported to the frontend before any query-wide sibling cancellation"
        );

        sibling_token.complete();
        let report_decision = service.queries.finish_fragment_for_report(query_id);
        service.queries.unregister_fragment(sibling_finst);
        service
            .queries
            .cleanup_after_fragment_report(query_id, report_decision);
    }

    #[test]
    fn failure_trigger_skips_ineligible_fragment_and_fails_exactly_one_eligible_fragment() {
        let trigger = std::env::temp_dir().join(format!(
            "novarocks-fragment-failure-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        std::fs::write(&trigger, b"step-token-17").expect("arm fragment failure");
        let first = values_result_request(84_000, 84_002);
        let first = prepare_fragment(first.into_submission(), FragmentPrepareContext::default())
            .expect("first fragment prepares");

        let (first, release) =
            start_with_fragment_failure_trigger(first, Some(trigger.as_path()), false);
        assert!(
            release.is_none(),
            "an ineligible result fragment must not claim the trigger"
        );
        assert!(
            trigger.exists(),
            "an ineligible result fragment must leave the trigger armed"
        );
        let first = first.join();
        assert!(
            matches!(first.outcome(), FragmentOutcome::Succeeded),
            "the ineligible fragment must run normally: {first:?}"
        );

        let second = values_result_request(84_100, 84_102);
        let second = prepare_fragment(second.into_submission(), FragmentPrepareContext::default())
            .expect("second fragment prepares");
        let (second, release) =
            start_with_fragment_failure_trigger(second, Some(trigger.as_path()), true);
        assert!(
            second.submitted_driver_count() > 0,
            "the injected failure must happen after the fragment enters started state"
        );
        std::fs::write(trigger.with_extension("release"), b"step-token-17")
            .expect("release fragment failure");
        assert_eq!(
            release
                .expect("armed fragment has a pending release")
                .wait()
                .expect("matching release token"),
            "step-token-17"
        );
        let failed = second.join();
        assert!(matches!(
            failed.outcome(),
            FragmentOutcome::Failed(error)
                if error.detail() == FRAGMENT_EXECUTOR_FAILURE_MESSAGE
        ));
        assert!(!trigger.exists(), "the trigger must be consumed once");

        let third = values_result_request(84_200, 84_202);
        let third = prepare_fragment(third.into_submission(), FragmentPrepareContext::default())
            .expect("third fragment prepares");
        let (third, release) =
            start_with_fragment_failure_trigger(third, Some(trigger.as_path()), true);
        assert!(
            release.is_none(),
            "the consumed trigger must not create another release rendezvous"
        );
        let succeeded = third.join();
        assert!(
            matches!(succeeded.outcome(), FragmentOutcome::Succeeded),
            "the consumed trigger must not poison later fragments: {succeeded:?}"
        );
    }
}
