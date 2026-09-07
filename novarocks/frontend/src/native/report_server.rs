//! Frontend-owned report-only native endpoint.

use std::collections::BTreeMap;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use crate::topology::ClusterBackendService;
use novarocks_proto_codec::membership::{
    BackendAnnounceRejectionReason, BackendAnnounceRequest, BackendAnnounceResult,
};
use novarocks_proto_codec::{ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{catalog, filter, novarocks as proto};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;

use crate::coordinator::{
    QueryLifecycleConvergenceErrorSource, QueryLifecycleConvergenceReader,
    QueryLifecycleConvergenceSnapshot, RuntimeFilterTerminalRollupSnapshot,
    RuntimeFilterTerminalRollupUnavailable,
};
use crate::query_execution::runtime_filter_terminal_rollup::{
    RuntimeFilterParticipantTerminalDetails, RuntimeFilterParticipantTerminalTelemetry,
    RuntimeFilterParticipantTerminalTelemetryValue, RuntimeFilterTerminalTotals,
    RuntimeFilterTerminalTotalsTelemetry, RuntimeFilterTerminalTotalsUnavailable,
};

use super::generated::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};
use super::transport::FrontendNativeTransport;
use novarocks_native_trust::{BoxedNativeIo, NativeServerAdmission, NativeTrust};

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Role-local wrapper supplying the connection marker Tonic requires after
/// the trust adapter has selected plaintext or completed TLS.
struct FrontendReportNativeIo(BoxedNativeIo);

impl AsyncRead for FrontendReportNativeIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.0.as_mut()).poll_read(context, buffer)
    }
}

impl AsyncWrite for FrontendReportNativeIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.0.as_mut()).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.0.as_mut()).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.0.as_mut()).poll_shutdown(context)
    }
}

impl Connected for FrontendReportNativeIo {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

pub(crate) const LIFECYCLE_CONVERGENCE_DEBUG_PATH: &str = "/debug/query-lifecycle/latest";

pub(crate) fn lifecycle_convergence_debug_enabled() -> bool {
    cfg!(debug_assertions)
        && std::env::var_os(novarocks_failpoint::QUERY_LIFECYCLE_FAULT_DIR_ENV).is_some()
}

#[derive(serde::Serialize)]
struct LifecycleConvergenceDebugSnapshot {
    execution_id: String,
    query_process_namespace: String,
    query_local_sequence: u64,
    query_attempt_id: u64,
    error_source: Option<&'static str>,
    runtime_filter: RuntimeFilterTerminalRollupDebug,
    /// This endpoint intentionally exposes only query-scoped immutable
    /// terminal evidence. Process metrics are not an acceptable substitute.
    metrics: BTreeMap<String, i64>,
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[expect(
    clippy::large_enum_variant,
    reason = "The typed frontend protocol model intentionally keeps payloads inline."
)]
enum RuntimeFilterTerminalRollupDebug {
    Available {
        participants: Vec<RuntimeFilterParticipantTerminalDebug>,
        totals: RuntimeFilterTerminalTotalsDebug,
    },
    Unavailable {
        reason: &'static str,
    },
}

#[derive(serde::Serialize)]
struct RuntimeFilterParticipantTerminalDebug {
    participant: RuntimeFilterParticipantDebug,
    telemetry: RuntimeFilterParticipantTelemetryDebug,
}

#[derive(serde::Serialize)]
struct RuntimeFilterParticipantDebug {
    process_id: String,
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum RuntimeFilterParticipantTelemetryDebug {
    Available {
        channels: Vec<RuntimeFilterChannelDebug>,
        producer_streams: Vec<RuntimeFilterProducerStreamDebug>,
        transport_routes: Vec<RuntimeFilterTransportRouteDebug>,
        consumers: Vec<RuntimeFilterConsumerDebug>,
    },
    Unavailable {
        stage: String,
        code: String,
    },
}

#[derive(serde::Serialize)]
struct RuntimeFilterChannelDebug {
    channel_binding_id: u32,
    channel_id: u32,
    install_state: String,
    terminal_state: String,
    latest_published_logical_version: Option<u64>,
    published_count: u64,
    completed_count: u64,
    unavailable_count: u64,
    cancelled_count: u64,
}

#[derive(serde::Serialize)]
struct RuntimeFilterProducerStreamDebug {
    channel_binding_id: u32,
    channel_id: u32,
    producer_fragment_instance_id: Option<RuntimeFilterUniqueIdDebug>,
    partition_id: u32,
    latest_accepted_sequence: Option<u64>,
    accepted_count: u64,
    duplicate_count: u64,
    stale_count: u64,
    conflict_count: u64,
    resource_limit_count: u64,
}

#[derive(serde::Serialize)]
struct RuntimeFilterTransportRouteDebug {
    channel_binding_id: u32,
    channel_id: u32,
    route_edge_id: u64,
    sent_count: u64,
    sent_bytes: u64,
    retried_count: u64,
    retried_bytes: u64,
    acked_count: u64,
    acked_bytes: u64,
    fail_open_count: u64,
    fail_open_bytes: u64,
}

#[derive(serde::Serialize)]
struct RuntimeFilterConsumerDebug {
    channel_binding_id: u32,
    channel_id: u32,
    consumer_binding_id: u32,
    fragment_instance_id: Option<RuntimeFilterUniqueIdDebug>,
    latest_delivered_logical_version: Option<u64>,
    latest_applied_logical_version: Option<u64>,
    subscription_terminal: String,
    row_evaluations: u64,
    input_rows: u64,
    output_rows: u64,
    scan_evaluated: u64,
    scan_kept: u64,
    scan_pruned: u64,
    scan_not_evaluated: u64,
    scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedDebug,
}

#[derive(serde::Serialize)]
struct RuntimeFilterUniqueIdDebug {
    high: i64,
    low: i64,
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[expect(
    clippy::large_enum_variant,
    reason = "The typed frontend protocol model intentionally keeps payloads inline."
)]
enum RuntimeFilterTerminalTotalsDebug {
    Available {
        channels: RuntimeFilterChannelTotalsDebug,
        producer_streams: RuntimeFilterProducerStreamTotalsDebug,
        transport_routes: RuntimeFilterTransportRouteTotalsDebug,
        consumers: RuntimeFilterConsumerTotalsDebug,
    },
    Unavailable {
        reason: &'static str,
    },
}

#[derive(serde::Serialize)]
struct RuntimeFilterChannelTotalsDebug {
    count: u64,
    published_count: u64,
    completed_count: u64,
    unavailable_count: u64,
    cancelled_count: u64,
}

#[derive(serde::Serialize)]
struct RuntimeFilterProducerStreamTotalsDebug {
    count: u64,
    accepted_count: u64,
    duplicate_count: u64,
    stale_count: u64,
    conflict_count: u64,
    resource_limit_count: u64,
}

#[derive(serde::Serialize)]
struct RuntimeFilterTransportRouteTotalsDebug {
    count: u64,
    sent_count: u64,
    sent_bytes: u64,
    retried_count: u64,
    retried_bytes: u64,
    acked_count: u64,
    acked_bytes: u64,
    fail_open_count: u64,
    fail_open_bytes: u64,
}

#[derive(serde::Serialize)]
struct RuntimeFilterConsumerTotalsDebug {
    count: u64,
    row_evaluations: u64,
    input_rows: u64,
    output_rows: u64,
    scan_evaluated: u64,
    scan_kept: u64,
    scan_pruned: u64,
    scan_not_evaluated: u64,
    scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedDebug,
}

#[derive(serde::Serialize)]
struct RuntimeFilterScanNotEvaluatedDebug {
    unit_facts_missing: u64,
    column_facts_missing: u64,
    data_type_unsupported: u64,
    predicate_capability_unsupported: u64,
    resource_unavailable: u64,
    snapshot_unavailable: u64,
    snapshot_timed_out: u64,
    snapshot_not_published: u64,
}

pub(crate) fn lifecycle_convergence_debug_json(
    snapshot: QueryLifecycleConvergenceSnapshot,
) -> serde_json::Value {
    serde_json::to_value(lifecycle_convergence_debug_snapshot(snapshot))
        .expect("frontend lifecycle convergence debug snapshot is serializable")
}

fn lifecycle_convergence_debug_snapshot(
    snapshot: QueryLifecycleConvergenceSnapshot,
) -> LifecycleConvergenceDebugSnapshot {
    let attribution = snapshot
        .execution_id
        .query_id()
        .process_attribution()
        .expect("frontend query allocator always emits a positive local sequence");
    let error_source = snapshot.error_source.map(|source| match source {
        QueryLifecycleConvergenceErrorSource::BackendAttestation => "backend-attestation",
        QueryLifecycleConvergenceErrorSource::FrontendLiveness => "frontend-liveness",
        QueryLifecycleConvergenceErrorSource::NoOutcome => "no-outcome",
    });
    LifecycleConvergenceDebugSnapshot {
        execution_id: format!(
            "{}:{}:{}",
            snapshot.execution_id.query_id().high(),
            snapshot.execution_id.query_id().low(),
            snapshot.execution_id.attempt_id().get()
        ),
        query_process_namespace: attribution.namespace().to_string(),
        query_local_sequence: attribution.sequence().get(),
        query_attempt_id: snapshot.execution_id.attempt_id().get(),
        error_source,
        runtime_filter: runtime_filter_terminal_rollup_debug(snapshot.runtime_filter),
        metrics: lifecycle_metric_map(snapshot.metrics),
    }
}

fn runtime_filter_terminal_rollup_debug(
    snapshot: RuntimeFilterTerminalRollupSnapshot,
) -> RuntimeFilterTerminalRollupDebug {
    match snapshot {
        RuntimeFilterTerminalRollupSnapshot::Available(rollup) => {
            RuntimeFilterTerminalRollupDebug::Available {
                participants: rollup
                    .participants
                    .into_iter()
                    .map(runtime_filter_participant_debug)
                    .collect(),
                totals: runtime_filter_totals_debug(rollup.totals),
            }
        }
        RuntimeFilterTerminalRollupSnapshot::Unavailable(reason) => {
            RuntimeFilterTerminalRollupDebug::Unavailable {
                reason: runtime_filter_rollup_unavailable_reason(reason),
            }
        }
    }
}

fn runtime_filter_rollup_unavailable_reason(
    reason: RuntimeFilterTerminalRollupUnavailable,
) -> &'static str {
    match reason {
        RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete => {
            "terminal-outcomes-incomplete"
        }
    }
}

fn runtime_filter_participant_debug(
    participant: RuntimeFilterParticipantTerminalTelemetry,
) -> RuntimeFilterParticipantTerminalDebug {
    let participant_identity = RuntimeFilterParticipantDebug {
        process_id: participant.participant.process_id.to_string(),
    };
    let telemetry = match participant.telemetry {
        RuntimeFilterParticipantTerminalTelemetryValue::Available(details) => {
            runtime_filter_participant_available_debug(details)
        }
        RuntimeFilterParticipantTerminalTelemetryValue::Unavailable(unavailable) => {
            RuntimeFilterParticipantTelemetryDebug::Unavailable {
                stage: unavailable.stage,
                code: unavailable.code,
            }
        }
    };
    RuntimeFilterParticipantTerminalDebug {
        participant: participant_identity,
        telemetry,
    }
}

fn runtime_filter_participant_available_debug(
    details: RuntimeFilterParticipantTerminalDetails,
) -> RuntimeFilterParticipantTelemetryDebug {
    RuntimeFilterParticipantTelemetryDebug::Available {
        channels: details
            .channels
            .into_iter()
            .map(|channel| RuntimeFilterChannelDebug {
                channel_binding_id: channel.channel_binding_id,
                channel_id: channel.channel_id,
                install_state: proto::QueryTerminalRuntimeFilterChannelInstallStateV1::try_from(
                    channel.install_state,
                )
                .expect("validated runtime-filter channel install state")
                .as_str_name()
                .to_owned(),
                terminal_state: proto::QueryTerminalRuntimeFilterChannelTerminalStateV1::try_from(
                    channel.terminal_state,
                )
                .expect("validated runtime-filter channel terminal state")
                .as_str_name()
                .to_owned(),
                latest_published_logical_version: channel.latest_published_logical_version,
                published_count: channel.published_count,
                completed_count: channel.completed_count,
                unavailable_count: channel.unavailable_count,
                cancelled_count: channel.cancelled_count,
            })
            .collect(),
        producer_streams: details
            .producer_streams
            .into_iter()
            .map(|stream| RuntimeFilterProducerStreamDebug {
                channel_binding_id: stream.channel_binding_id,
                channel_id: stream.channel_id,
                producer_fragment_instance_id: stream
                    .producer_fragment_instance_id
                    .map(runtime_filter_unique_id_debug),
                partition_id: stream.partition_id,
                latest_accepted_sequence: stream.latest_accepted_sequence,
                accepted_count: stream.accepted_count,
                duplicate_count: stream.duplicate_count,
                stale_count: stream.stale_count,
                conflict_count: stream.conflict_count,
                resource_limit_count: stream.resource_limit_count,
            })
            .collect(),
        transport_routes: details
            .transport_routes
            .into_iter()
            .map(|route| RuntimeFilterTransportRouteDebug {
                channel_binding_id: route.channel_binding_id,
                channel_id: route.channel_id,
                route_edge_id: route.route_edge_id,
                sent_count: route.sent_count,
                sent_bytes: route.sent_bytes,
                retried_count: route.retried_count,
                retried_bytes: route.retried_bytes,
                acked_count: route.acked_count,
                acked_bytes: route.acked_bytes,
                fail_open_count: route.fail_open_count,
                fail_open_bytes: route.fail_open_bytes,
            })
            .collect(),
        consumers: details
            .consumers
            .into_iter()
            .map(|consumer| RuntimeFilterConsumerDebug {
                channel_binding_id: consumer.channel_binding_id,
                channel_id: consumer.channel_id,
                consumer_binding_id: consumer.consumer_binding_id,
                fragment_instance_id: consumer
                    .fragment_instance_id
                    .map(runtime_filter_unique_id_debug),
                latest_delivered_logical_version: consumer.latest_delivered_logical_version,
                latest_applied_logical_version: consumer.latest_applied_logical_version,
                subscription_terminal:
                    proto::QueryTerminalRuntimeFilterSubscriptionTerminalV1::try_from(
                        consumer.subscription_terminal,
                    )
                    .expect("validated runtime-filter subscription terminal state")
                    .as_str_name()
                    .to_owned(),
                row_evaluations: consumer.row_evaluations,
                input_rows: consumer.input_rows,
                output_rows: consumer.output_rows,
                scan_evaluated: consumer.scan_evaluated,
                scan_kept: consumer.scan_kept,
                scan_pruned: consumer.scan_pruned,
                scan_not_evaluated: consumer.scan_not_evaluated,
                scan_not_evaluated_reasons: runtime_filter_scan_not_evaluated_debug(
                    consumer
                        .scan_not_evaluated_reasons
                        .expect("validated runtime-filter consumer scan reasons"),
                ),
            })
            .collect(),
    }
}

fn runtime_filter_unique_id_debug(
    id: novarocks_proto_models::common::UniqueId,
) -> RuntimeFilterUniqueIdDebug {
    RuntimeFilterUniqueIdDebug {
        high: id.hi,
        low: id.lo,
    }
}

fn runtime_filter_scan_not_evaluated_debug(
    reasons: proto::QueryTerminalRuntimeFilterScanNotEvaluatedV1,
) -> RuntimeFilterScanNotEvaluatedDebug {
    RuntimeFilterScanNotEvaluatedDebug {
        unit_facts_missing: reasons.unit_facts_missing,
        column_facts_missing: reasons.column_facts_missing,
        data_type_unsupported: reasons.data_type_unsupported,
        predicate_capability_unsupported: reasons.predicate_capability_unsupported,
        resource_unavailable: reasons.resource_unavailable,
        snapshot_unavailable: reasons.snapshot_unavailable,
        snapshot_timed_out: reasons.snapshot_timed_out,
        snapshot_not_published: reasons.snapshot_not_published,
    }
}

fn runtime_filter_totals_debug(
    totals: RuntimeFilterTerminalTotalsTelemetry,
) -> RuntimeFilterTerminalTotalsDebug {
    match totals {
        RuntimeFilterTerminalTotalsTelemetry::Available(totals) => {
            runtime_filter_available_totals_debug(totals)
        }
        RuntimeFilterTerminalTotalsTelemetry::Unavailable(reason) => {
            RuntimeFilterTerminalTotalsDebug::Unavailable {
                reason: match reason {
                    RuntimeFilterTerminalTotalsUnavailable::ParticipantTelemetryUnavailable => {
                        "participant-telemetry-unavailable"
                    }
                    RuntimeFilterTerminalTotalsUnavailable::CounterOverflow => "counter-overflow",
                },
            }
        }
    }
}

fn runtime_filter_available_totals_debug(
    totals: RuntimeFilterTerminalTotals,
) -> RuntimeFilterTerminalTotalsDebug {
    RuntimeFilterTerminalTotalsDebug::Available {
        channels: RuntimeFilterChannelTotalsDebug {
            count: totals.channels.count,
            published_count: totals.channels.published_count,
            completed_count: totals.channels.completed_count,
            unavailable_count: totals.channels.unavailable_count,
            cancelled_count: totals.channels.cancelled_count,
        },
        producer_streams: RuntimeFilterProducerStreamTotalsDebug {
            count: totals.producer_streams.count,
            accepted_count: totals.producer_streams.accepted_count,
            duplicate_count: totals.producer_streams.duplicate_count,
            stale_count: totals.producer_streams.stale_count,
            conflict_count: totals.producer_streams.conflict_count,
            resource_limit_count: totals.producer_streams.resource_limit_count,
        },
        transport_routes: RuntimeFilterTransportRouteTotalsDebug {
            count: totals.transport_routes.count,
            sent_count: totals.transport_routes.sent_count,
            sent_bytes: totals.transport_routes.sent_bytes,
            retried_count: totals.transport_routes.retried_count,
            retried_bytes: totals.transport_routes.retried_bytes,
            acked_count: totals.transport_routes.acked_count,
            acked_bytes: totals.transport_routes.acked_bytes,
            fail_open_count: totals.transport_routes.fail_open_count,
            fail_open_bytes: totals.transport_routes.fail_open_bytes,
        },
        consumers: RuntimeFilterConsumerTotalsDebug {
            count: totals.consumers.count,
            row_evaluations: totals.consumers.row_evaluations,
            input_rows: totals.consumers.input_rows,
            output_rows: totals.consumers.output_rows,
            scan_evaluated: totals.consumers.scan_evaluated,
            scan_kept: totals.consumers.scan_kept,
            scan_pruned: totals.consumers.scan_pruned,
            scan_not_evaluated: totals.consumers.scan_not_evaluated,
            scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedDebug {
                unit_facts_missing: totals
                    .consumers
                    .scan_not_evaluated_reasons
                    .unit_facts_missing,
                column_facts_missing: totals
                    .consumers
                    .scan_not_evaluated_reasons
                    .column_facts_missing,
                data_type_unsupported: totals
                    .consumers
                    .scan_not_evaluated_reasons
                    .data_type_unsupported,
                predicate_capability_unsupported: totals
                    .consumers
                    .scan_not_evaluated_reasons
                    .predicate_capability_unsupported,
                resource_unavailable: totals
                    .consumers
                    .scan_not_evaluated_reasons
                    .resource_unavailable,
                snapshot_unavailable: totals
                    .consumers
                    .scan_not_evaluated_reasons
                    .snapshot_unavailable,
                snapshot_timed_out: totals
                    .consumers
                    .scan_not_evaluated_reasons
                    .snapshot_timed_out,
                snapshot_not_published: totals
                    .consumers
                    .scan_not_evaluated_reasons
                    .snapshot_not_published,
            },
        },
    }
}

fn lifecycle_metric_map(
    metrics: crate::metrics::FrontendProcessQueryCountersSnapshot,
) -> BTreeMap<String, i64> {
    [
        ("active_attempts", metrics.active_attempts as i64),
        ("init_applied", metrics.init_applied as i64),
        ("init_idempotent", metrics.init_idempotent as i64),
        ("init_failed", metrics.init_failed as i64),
        ("control_ready", metrics.control_ready as i64),
        ("attach_failed", metrics.attach_failed as i64),
        ("heartbeat_timeouts", metrics.heartbeat_timeouts as i64),
        ("coordinator_lost", metrics.coordinator_lost as i64),
        ("local_failures", metrics.local_failures as i64),
        (
            "backend_epoch_mismatches",
            metrics.backend_epoch_mismatches as i64,
        ),
        ("cleanup_failures", metrics.cleanup_failures as i64),
        (
            "terminal_locally_drained",
            metrics.terminal_locally_drained as i64,
        ),
        (
            "terminal_snapshots_accepted",
            metrics.terminal_snapshots_accepted as i64,
        ),
        (
            "terminal_snapshots_idempotent",
            metrics.terminal_snapshots_idempotent as i64,
        ),
        (
            "terminal_snapshot_conflicts",
            metrics.terminal_snapshot_conflicts as i64,
        ),
        (
            "terminal_finalize_failures",
            metrics.terminal_finalize_failures as i64,
        ),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value))
    .collect()
}

#[derive(Clone)]
struct FrontendReportService {
    membership: Arc<ClusterBackendService>,
    deployment_id: String,
}

impl FrontendReportService {
    fn rejected(rpc_name: &str) -> tonic::Status {
        tonic::Status::failed_precondition(format!(
            "report-only NovaRocksGrpc endpoint rejects local execution RPC: {rpc_name}"
        ))
    }
}

#[tonic::async_trait]
impl NovaRocksGrpc for FrontendReportService {
    type ExchangeStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::ExchangeResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;
    type SubscribeTaskStatusStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::TaskStatusStreamEvent, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn announce_backend(
        &self,
        request: tonic::Request<proto::AnnounceBackendRequest>,
    ) -> Result<tonic::Response<proto::AnnounceBackendResponse>, tonic::Status> {
        let announce = BackendAnnounceRequest::parse(request.into_inner())
            .map_err(status_from_contract_error)?;
        let descriptor = announce.descriptor().map_err(status_from_contract_error)?;
        let result = if descriptor.deployment_id() != self.deployment_id {
            BackendAnnounceResult::rejected(
                BackendAnnounceRejectionReason::DeploymentMismatch,
                "backend deployment does not match frontend deployment",
            )
        } else {
            self.membership
                .record_announce(
                    descriptor,
                    announce
                        .reported_state()
                        .map_err(status_from_contract_error)?,
                )
                .map(|()| BackendAnnounceResult::accepted(self.membership.announce_lease_ttl_ms()))
                .unwrap_or_else(|error| {
                    BackendAnnounceResult::rejected(
                        BackendAnnounceRejectionReason::DescriptorConflict,
                        error,
                    )
                })
        }
        .map_err(status_from_contract_error)?;
        Ok(tonic::Response::new(result.to_proto()))
    }

    async fn exchange(
        &self,
        _request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        Err(Self::rejected("Exchange"))
    }

    async fn exchange_unary(
        &self,
        _request: tonic::Request<proto::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::ExchangeResponse>, tonic::Status> {
        Err(Self::rejected("ExchangeUnary"))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        _request: tonic::Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        Err(Self::rejected("TransmitRuntimeFilterEnvelope"))
    }

    async fn lookup(
        &self,
        _request: tonic::Request<filter::LookupRequest>,
    ) -> Result<tonic::Response<filter::LookupResponse>, tonic::Status> {
        Err(Self::rejected("Lookup"))
    }

    async fn fetch_result(
        &self,
        _request: tonic::Request<proto::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        Err(Self::rejected("FetchResult"))
    }

    async fn prune_catalogs(
        &self,
        _request: tonic::Request<catalog::PruneCatalogsRequest>,
    ) -> Result<tonic::Response<catalog::PruneCatalogsResponse>, tonic::Status> {
        Err(Self::rejected("PruneCatalogs"))
    }

    async fn heartbeat(
        &self,
        _request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        Err(Self::rejected("Heartbeat"))
    }

    async fn apply_task_operations(
        &self,
        _request: tonic::Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        Err(Self::rejected("ApplyTaskOperations"))
    }

    async fn subscribe_task_status(
        &self,
        _request: tonic::Request<proto::SubscribeTaskStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTaskStatusStream>, tonic::Status> {
        Err(Self::rejected("SubscribeTaskStatus"))
    }

    async fn fetch_task_dynamic_filters(
        &self,
        _request: tonic::Request<proto::FetchTaskDynamicFiltersRequest>,
    ) -> Result<tonic::Response<proto::FetchTaskDynamicFiltersResponse>, tonic::Status> {
        Err(Self::rejected("FetchTaskDynamicFilters"))
    }

    async fn get_final_task_info(
        &self,
        _request: tonic::Request<proto::GetFinalTaskInfoRequest>,
    ) -> Result<tonic::Response<proto::GetFinalTaskInfoResponse>, tonic::Status> {
        Err(Self::rejected("GetFinalTaskInfo"))
    }

    async fn fetch_task_result(
        &self,
        _request: tonic::Request<proto::FetchTaskResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        Err(Self::rejected("FetchTaskResult"))
    }
}

fn status_from_contract_error(error: ProtocolError) -> tonic::Status {
    let detail = error.detail().to_string();
    match error.kind() {
        ProtocolErrorKind::MissingField
        | ProtocolErrorKind::InvalidEnum
        | ProtocolErrorKind::InvalidValue
        | ProtocolErrorKind::OutOfRange
        | ProtocolErrorKind::DuplicateField
        | ProtocolErrorKind::InconsistentFields
        | ProtocolErrorKind::Unsupported
        | ProtocolErrorKind::VersionMismatch => tonic::Status::invalid_argument(detail),
        ProtocolErrorKind::Conflict => tonic::Status::already_exists(detail),
        ProtocolErrorKind::Capacity => tonic::Status::resource_exhausted(detail),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use super::super::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;
    use super::{
        FrontendNativeTransport, FrontendReportServerHandle, lifecycle_convergence_debug_snapshot,
    };
    use crate::coordinator::{
        QueryLifecycleConvergenceReader, RuntimeFilterTerminalRollupSnapshot,
        RuntimeFilterTerminalRollupUnavailable,
    };
    use crate::metrics::FrontendProcessQueryCountersSnapshot;
    use crate::query_execution::runtime_filter_terminal_rollup::{
        RuntimeFilterParticipantTerminalDetails, RuntimeFilterParticipantTerminalTelemetry,
        RuntimeFilterParticipantTerminalTelemetryValue, RuntimeFilterTerminalParticipant,
        RuntimeFilterTerminalRollup, RuntimeFilterTerminalTotals,
        RuntimeFilterTerminalTotalsTelemetry, RuntimeFilterTerminalTotalsUnavailable,
    };
    use crate::topology::ClusterBackendService;
    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_proto_models::novarocks as proto;
    use novarocks_secret::SecretValue;
    use novarocks_types::QueryId;

    fn test_trust() -> Arc<NativeTrust> {
        Arc::new(NativeTrust::new(
            DeploymentId::parse("frontend-report-test").expect("deployment"),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .expect("secret"),
            NativeCallerSubject::parse("be@127.0.0.1:19040").expect("subject"),
            NativeTransportMode::Disabled,
        ))
    }

    struct EmptyConvergenceReader;

    impl QueryLifecycleConvergenceReader for EmptyConvergenceReader {
        fn latest_convergence_snapshot(
            &self,
        ) -> Option<crate::coordinator::QueryLifecycleConvergenceSnapshot> {
            None
        }
    }

    fn debug_snapshot_with_runtime_filter(
        runtime_filter: RuntimeFilterTerminalRollupSnapshot,
    ) -> crate::coordinator::QueryLifecycleConvergenceSnapshot {
        crate::coordinator::QueryLifecycleConvergenceSnapshot {
            execution_id: QueryExecutionId::new(
                QueryId::new(51, 52),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("execution id"),
            error_source: None,
            primary_error: None,
            runtime_filter,
            metrics: FrontendProcessQueryCountersSnapshot::default(),
        }
    }

    fn available_runtime_filter_rollup(
        totals: RuntimeFilterTerminalTotalsTelemetry,
    ) -> RuntimeFilterTerminalRollup {
        RuntimeFilterTerminalRollup {
            participants: vec![RuntimeFilterParticipantTerminalTelemetry {
                participant: RuntimeFilterTerminalParticipant {
                    process_id: novarocks_types::BackendProcessId::new_v7(),
                },
                telemetry: RuntimeFilterParticipantTerminalTelemetryValue::Available(
                    RuntimeFilterParticipantTerminalDetails {
                        channels: vec![proto::QueryTerminalRuntimeFilterChannelV1 {
                            channel_binding_id: 3,
                            channel_id: 5,
                            install_state:
                                proto::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed
                                    as i32,
                            terminal_state:
                                proto::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed
                                    as i32,
                            latest_published_logical_version: Some(4),
                            published_count: 2,
                            completed_count: 1,
                            unavailable_count: 0,
                            cancelled_count: 0,
                        }],
                        producer_streams: vec![proto::QueryTerminalRuntimeFilterProducerStreamV1 {
                            channel_binding_id: 3,
                            channel_id: 5,
                            producer_fragment_instance_id: Some(
                                novarocks_proto_models::common::UniqueId { hi: 9, lo: 10 },
                            ),
                            partition_id: 0,
                            latest_accepted_sequence: Some(0),
                            accepted_count: 1,
                            duplicate_count: 1,
                            stale_count: 0,
                            conflict_count: 0,
                            resource_limit_count: 0,
                        }],
                        transport_routes: vec![proto::QueryTerminalRuntimeFilterTransportRouteV1 {
                            channel_binding_id: 3,
                            channel_id: 5,
                            route_edge_id: 13,
                            sent_count: 1,
                            sent_bytes: 64,
                            retried_count: 1,
                            retried_bytes: 64,
                            acked_count: 1,
                            acked_bytes: 64,
                            fail_open_count: 0,
                            fail_open_bytes: 0,
                        }],
                        consumers: vec![proto::QueryTerminalRuntimeFilterConsumerV1 {
                            channel_binding_id: 3,
                            channel_id: 5,
                            consumer_binding_id: 8,
                            fragment_instance_id: Some(novarocks_proto_models::common::UniqueId {
                                hi: 12,
                                lo: 14,
                            }),
                            latest_delivered_logical_version: Some(4),
                            latest_applied_logical_version: Some(4),
                            subscription_terminal:
                                proto::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed
                                    as i32,
                            row_evaluations: 9,
                            input_rows: 8,
                            output_rows: 7,
                            scan_evaluated: 6,
                            scan_kept: 4,
                            scan_pruned: 2,
                            scan_not_evaluated: 0,
                            scan_not_evaluated_reasons: Some(Default::default()),
                        }],
                    },
                ),
            }],
            totals,
        }
    }

    #[test]
    fn query_lifecycle_convergence_debug_exposes_runtime_filter_details_and_totals() {
        let mut totals = RuntimeFilterTerminalTotals::default();
        totals.channels.count = 1;
        totals.channels.published_count = 2;
        totals.producer_streams.duplicate_count = 1;
        totals.transport_routes.retried_count = 1;
        totals.consumers.output_rows = 7;
        let value = serde_json::to_value(lifecycle_convergence_debug_snapshot(
            debug_snapshot_with_runtime_filter(RuntimeFilterTerminalRollupSnapshot::Available(
                available_runtime_filter_rollup(RuntimeFilterTerminalTotalsTelemetry::Available(
                    totals,
                )),
            )),
        ))
        .expect("serialize debug snapshot");

        assert_eq!(value["execution_id"], "51:52:1");
        assert_eq!(value["query_process_namespace"], "0x0000000000000033");
        assert_eq!(value["query_local_sequence"], 52);
        assert_eq!(value["query_attempt_id"], 1);
        assert_eq!(value["runtime_filter"]["kind"], "available");
        assert!(
            value["runtime_filter"]["participants"][0]["participant"]["process_id"]
                .as_str()
                .is_some()
        );
        assert_eq!(
            value["runtime_filter"]["participants"][0]["telemetry"]["channels"][0]["terminal_state"],
            "QUERY_TERMINAL_RUNTIME_FILTER_CHANNEL_TERMINAL_STATE_V1_COMPLETED"
        );
        assert_eq!(
            value["runtime_filter"]["participants"][0]["telemetry"]["transport_routes"][0]["retried_count"],
            1
        );
        assert_eq!(
            value["runtime_filter"]["totals"]["transport_routes"]["retried_count"],
            1
        );
        assert_eq!(
            value["runtime_filter"]["totals"]["consumers"]["output_rows"],
            7
        );
        assert_eq!(value["metrics"]["active_attempts"], 0);
    }

    #[test]
    fn query_lifecycle_convergence_debug_marks_incomplete_terminal_rollup_unavailable() {
        let value = serde_json::to_value(lifecycle_convergence_debug_snapshot(
            debug_snapshot_with_runtime_filter(RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            )),
        ))
        .expect("serialize debug snapshot");

        assert_eq!(value["runtime_filter"]["kind"], "unavailable");
        assert_eq!(
            value["runtime_filter"]["reason"],
            "terminal-outcomes-incomplete"
        );
    }

    #[test]
    fn query_lifecycle_convergence_debug_keeps_participant_truth_when_totals_overflow() {
        let value = serde_json::to_value(lifecycle_convergence_debug_snapshot(
            debug_snapshot_with_runtime_filter(RuntimeFilterTerminalRollupSnapshot::Available(
                available_runtime_filter_rollup(RuntimeFilterTerminalTotalsTelemetry::Unavailable(
                    RuntimeFilterTerminalTotalsUnavailable::CounterOverflow,
                )),
            )),
        ))
        .expect("serialize debug snapshot");

        assert_eq!(value["runtime_filter"]["kind"], "available");
        assert_eq!(
            value["runtime_filter"]["participants"][0]["telemetry"]["producer_streams"][0]["duplicate_count"],
            1
        );
        assert_eq!(value["runtime_filter"]["totals"]["kind"], "unavailable");
        assert_eq!(
            value["runtime_filter"]["totals"]["reason"],
            "counter-overflow"
        );
    }

    #[tokio::test]
    async fn report_listener_rejects_missing_invalid_and_duplicate_authorization_before_dispatch() {
        let trust = test_trust();
        let mut server = FrontendReportServerHandle::start(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            Arc::new(ClusterBackendService::new_transient_for_test(1)),
            Arc::new(EmptyConvergenceReader),
            Arc::clone(&trust),
            FrontendNativeTransport::plaintext(),
        )
        .expect("start frontend report server");

        for (authorization, duplicate) in [
            (None, false),
            (Some("Bearer invalid"), false),
            (Some("Bearer invalid"), true),
        ] {
            let channel =
                tonic::transport::Endpoint::from_shared(format!("http://{}", server.bound_addr()))
                    .expect("report endpoint")
                    .connect()
                    .await
                    .expect("connect report client");
            let mut client = NovaRocksGrpcClient::new(channel);
            let mut request = tonic::Request::new(proto::AnnounceBackendRequest::default());
            if let Some(authorization) = authorization {
                request
                    .metadata_mut()
                    .append("authorization", authorization.parse().expect("metadata"));
            }
            if duplicate {
                request
                    .metadata_mut()
                    .append("authorization", "Bearer another".parse().expect("metadata"));
            }
            let error = client
                .announce_backend(request)
                .await
                .expect_err("listener must reject unauthenticated request");
            assert_eq!(error.code(), tonic::Code::Unauthenticated);
        }

        server.stop().expect("stop frontend report server");
    }

    #[tokio::test]
    async fn report_listener_rejects_unauthenticated_requests_without_exposing_management_metrics()
    {
        let convergence_reader: Arc<dyn QueryLifecycleConvergenceReader> =
            Arc::new(EmptyConvergenceReader);
        let trust = test_trust();
        let mut server = FrontendReportServerHandle::start(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            Arc::new(ClusterBackendService::new_transient_for_test(1)),
            convergence_reader,
            trust,
            FrontendNativeTransport::plaintext(),
        )
        .expect("start frontend report server");

        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://{}", server.bound_addr()))
                .expect("report endpoint")
                .connect()
                .await
                .expect("connect report client");
        let mut client = NovaRocksGrpcClient::new(channel);
        let error = client
            .announce_backend(proto::AnnounceBackendRequest::default())
            .await
            .expect_err("missing authorization must not reach the report domain");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);

        server.stop().expect("stop frontend report server");
    }
}

/// Instance-owned report listener. The host exposes only lifecycle methods,
/// never a Tonic service or a Core listener handle.
pub struct FrontendReportServerHandle {
    bound_addr: SocketAddr,
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl FrontendReportServerHandle {
    pub(crate) fn start(
        address: SocketAddr,
        membership: Arc<ClusterBackendService>,
        _convergence_reader: Arc<dyn QueryLifecycleConvergenceReader>,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, String> {
        let listener = TcpListener::bind(address).map_err(|error| {
            format!("bind frontend report endpoint on {address} failed: {error}")
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("set frontend report endpoint on {address} nonblocking failed: {error}")
        })?;
        let bound_addr = listener.local_addr().map_err(|error| {
            format!("read frontend report endpoint bound address failed: {error}")
        })?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let join_handle = std::thread::Builder::new()
            .name("frontend-report-grpc".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .worker_threads(8)
                        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
                        .build()
                        .map_err(|error| {
                            format!("build frontend report endpoint runtime failed: {error}")
                        })?;
                    runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create frontend report Tokio listener failed: {error}")
                        })?;
                        let incoming_adapter = native_transport.incoming_adapter();
                        let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel(64);
                        let accept_task = tokio::spawn(async move {
                            loop {
                                let (stream, _) = match listener.accept().await {
                                    Ok(value) => value,
                                    Err(error) => {
                                        let _ = incoming_tx
                                            .send(Err(io::Error::other(format!(
                                                "accept frontend report connection failed: {error}"
                                            ))))
                                            .await;
                                        return;
                                    }
                                };
                                match incoming_adapter.accept(stream).await {
                                    Ok(stream) => {
                                        if incoming_tx
                                            .send(Ok(FrontendReportNativeIo(stream)))
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                    Err(_) => {
                                        crate::metrics::observe_native_trust_transport_rejection(
                                            "report_listener",
                                        );
                                    }
                                }
                            }
                        });
                        let service = NovaRocksGrpcServer::new(FrontendReportService {
                            membership,
                            deployment_id: native_trust.deployment_id().as_str().to_string(),
                        })
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                        let serve = tonic::transport::Server::builder()
                            .layer(
                                NativeServerAdmission::new(native_trust.as_ref().clone())
                                    .listener_layer(),
                            )
                            .add_service(service)
                            .serve_with_incoming(ReceiverStream::new(incoming_rx));
                        tokio::pin!(serve);
                        let mut shutdown_rx = shutdown_rx;
                        let result = tokio::select! {
                            result = &mut serve => result.map_err(|error| {
                                format!("frontend report endpoint serve future failed: {error}")
                            }),
                            _ = async {
                                while !*shutdown_rx.borrow() {
                                    if shutdown_rx.changed().await.is_err() {
                                        break;
                                    }
                                }
                            } => Ok(()),
                        };
                        accept_task.abort();
                        result
                    })
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(Ok(())) => "frontend report endpoint exited unexpectedly".to_string(),
                    Ok(Err(error)) => error,
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|value| (*value).to_string())
                        })
                        .unwrap_or_else(|| "frontend report endpoint panicked".to_string()),
                };
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn frontend report endpoint: {error}"))?;
        Ok(Self {
            bound_addr,
            shutdown_tx: Some(shutdown_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    pub(crate) fn start_from_host(
        host: &str,
        port: u16,
        membership: Arc<ClusterBackendService>,
        convergence_reader: Arc<dyn QueryLifecycleConvergenceReader>,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, String> {
        Self::start(
            parse_bind_addr(host, port)?,
            membership,
            convergence_reader,
            native_trust,
            native_transport,
        )
    }

    pub const fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "frontend report endpoint thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for FrontendReportServerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn parse_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|error| format!("parse frontend report bind addr '{formatted}' failed: {error}"))
}
