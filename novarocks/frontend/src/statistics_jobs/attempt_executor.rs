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

//! Frontend-owned native execution adapter for durable ANALYZE attempts.
//!
//! Core owns the provider-neutral statistics program and its one-shot
//! prepare/finish boundary.  This adapter owns the only native mapping step:
//! it encodes the sealed prepared view before returning the resulting request
//! to the carrier-neutral query-execution service.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use crate::common::backend_topology::BackendTopologyService;
use crate::query_execution::service::QueryExecutionService;
use crate::statistics_jobs::application::{
    StatisticsApplicationError, StatisticsAttemptExecutor, StatisticsAttemptRequest,
    StatisticsCollectedAttempt,
};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorMutationOperationId, ConnectorRequestContext,
    ConnectorStatisticsLease, ConnectorTableHandle, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StatisticsCollectionRequest, StatisticsDataVersion,
    StatisticsMetric, StatisticsMetricRequest, StatisticsPublishPreparationRequest,
    StatisticsPublishRequest, StatisticsReconcileRequest,
};

/// Exact Frontend composition leaves retained by the durable ANALYZE worker.
/// Each collection takes a fresh live topology snapshot, while the table and
/// version pin are the immutable submission facts persisted by the worker.
#[derive(Clone)]
pub(crate) struct StatisticsAttemptExecutionPorts {
    execution_role: novarocks_types::ClusterRole,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    backend_topology: BackendTopologyService,
    query_execution: QueryExecutionService,
}

impl StatisticsAttemptExecutionPorts {
    pub(crate) fn new(
        execution_role: novarocks_types::ClusterRole,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        backend_topology: BackendTopologyService,
        query_execution: QueryExecutionService,
    ) -> Self {
        Self {
            execution_role,
            connector_control,
            backend_topology,
            query_execution,
        }
    }
}

/// Implements Core's durable-worker port while retaining the native encoder
/// exclusively in Frontend.
pub(crate) struct FrontendStatisticsAttemptExecutor {
    ports: StatisticsAttemptExecutionPorts,
}

impl FrontendStatisticsAttemptExecutor {
    pub(crate) fn new(ports: StatisticsAttemptExecutionPorts) -> Self {
        Self { ports }
    }

    fn collection_context() -> Result<ConnectorRequestContext, StatisticsApplicationError> {
        ConnectorRequestContext::try_new(
            Instant::now() + crate::query_execution::statistics::MAX_STATISTICS_ATTEMPT_DURATION,
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn table_and_version(
        request: &StatisticsAttemptRequest,
    ) -> Result<(ConnectorTableHandle, StatisticsDataVersion), StatisticsApplicationError> {
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(
            &request.table_pin.connector_instance_id,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let table = ConnectorTableHandle::try_new(
            instance_id,
            Bytes::copy_from_slice(&request.table_pin.table_handle),
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let version =
            StatisticsDataVersion::try_new(Bytes::copy_from_slice(&request.table_pin.data_version))
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        Ok((table, version))
    }

    fn metrics(
        request: &StatisticsAttemptRequest,
    ) -> Result<StatisticsMetricRequest, StatisticsApplicationError> {
        let mut metrics = vec![StatisticsMetric::RowCount];
        let columns = if request.metric_names.is_empty() {
            &request.table_pin.columns
        } else {
            &request.metric_names
        };
        for column in columns {
            let column: Arc<str> = Arc::from(column.as_str());
            metrics.extend([
                StatisticsMetric::NullCount {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::Minimum {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::Maximum {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::AverageSize {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::ThetaNdv { column },
            ]);
        }
        StatisticsMetricRequest::try_new(metrics)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn operation_id(request: &StatisticsAttemptRequest) -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes(*request.operation_id.as_bytes())
    }

    fn collected<'a>(
        collected: &'a dyn StatisticsCollectedAttempt,
    ) -> Result<&'a FrontendStatisticsCollectedAttempt, StatisticsApplicationError> {
        collected
            .as_any()
            .downcast_ref::<FrontendStatisticsCollectedAttempt>()
            .ok_or_else(|| {
                StatisticsApplicationError::new(
                    "statistics publication received a collection artifact from another executor",
                )
            })
    }

    fn outcome(
        outcome: ExternalMutationOutcome<novarocks_spi::connector::StatisticsReceipt>,
    ) -> Result<(), StatisticsApplicationError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Complete,
                ..
            } => Ok(()),
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Failed(failure),
                ..
            }
            | ExternalMutationOutcome::KnownUncommitted { failure } => {
                Err(StatisticsApplicationError::new(failure.to_string()))
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                Err(StatisticsApplicationError::reconcile(failure.to_string()))
            }
        }
    }
}

struct FrontendStatisticsCollectedAttempt {
    lease: ConnectorStatisticsLease,
    table: ConnectorTableHandle,
    result: novarocks_spi::connector::StatisticsCollectionResult,
    context: ConnectorRequestContext,
}

impl StatisticsCollectedAttempt for FrontendStatisticsCollectedAttempt {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatisticsAttemptExecutor for FrontendStatisticsAttemptExecutor {
    fn collect(
        &self,
        request: &StatisticsAttemptRequest,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsApplicationError> {
        let (table, data_version) = Self::table_and_version(request)?;
        let metrics = Self::metrics(request)?;
        let context = Self::collection_context()?;
        let planning_lease = self
            .ports
            .connector_control
            .acquire_current(table.owner())
            .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        let lease = planning_lease
            .derive_statistics_lease()
            .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        let plan = lease
            .prepare_collection(StatisticsCollectionRequest {
                operation_id: Self::operation_id(request),
                table: table.clone(),
                data_version,
                metrics,
                context: context.clone(),
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let program = crate::query_execution::statistics::StatisticsCollectionProgram::try_new(
            plan,
            crate::query_execution::statistics::StatisticsExecutionPolicy::try_new(
                crate::query_execution::statistics::StatisticsExecutionMode::DurableJobAttempt,
                crate::query_execution::statistics::MAX_STATISTICS_ATTEMPT_DURATION,
            )
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let topology = self
            .ports
            .backend_topology
            .snapshot()
            .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        let cancellation = crate::common::query_cancellation::QueryCancellationSource::new();
        let execution = crate::common::admitted_query_context::QueryExecutionContext::new(
            self.ports.execution_role,
            topology,
            Some(Instant::now() + program.policy().attempt_timeout()),
            cancellation.view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        );

        // The sequence is intentional: Core prepares immutable provider facts;
        // Frontend maps the sealed view; Core consumes the exact attachment.
        let prepared = crate::query_execution::statistics::prepare_statistics_collection_request(
            self.ports.connector_control.as_ref(),
            &execution,
            context.clone(),
            program,
            planning_lease,
        )
        .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        let native_attachment = crate::native::fragment_encoder::encode_native_fragment_bundle(
            prepared.encoding_view(),
        )
        .map_err(StatisticsApplicationError::transient)?;
        let distributed = prepared
            .finish(native_attachment)
            .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        let result = self
            .ports
            .query_execution
            .execute(distributed)
            .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_statistics)
            .map(|outcome| outcome.into_collection_result())
            .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        Ok(Box::new(FrontendStatisticsCollectedAttempt {
            lease,
            table,
            result,
            context,
        }))
    }

    fn prepare_publish(
        &self,
        request: &StatisticsAttemptRequest,
        collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsApplicationError> {
        let collected = Self::collected(collected)?;
        collected
            .lease
            .prepare_publish(StatisticsPublishPreparationRequest {
                operation_id: Self::operation_id(request),
                table: collected.table.clone(),
                result: collected.result.clone(),
                context: collected.context.clone(),
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn publish(
        &self,
        request: &StatisticsAttemptRequest,
        collected: &dyn StatisticsCollectedAttempt,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsApplicationError> {
        let collected = Self::collected(collected)?;
        Self::outcome(
            collected
                .lease
                .publish(StatisticsPublishRequest {
                    operation_id: Self::operation_id(request),
                    table: collected.table.clone(),
                    result: collected.result.clone(),
                    context: collected.context.clone(),
                    evidence: evidence.clone(),
                })
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        )
    }

    fn reconcile(
        &self,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsApplicationError> {
        let lease = self
            .ports
            .connector_control
            .acquire_current_statistics(&evidence.descriptor().instance_id)
            .map_err(|error| StatisticsApplicationError::reconcile(error.to_string()))?;
        Self::outcome(
            lease
                .reconcile(StatisticsReconcileRequest {
                    evidence: evidence.clone(),
                    context: Self::collection_context()?,
                })
                .map_err(|error| StatisticsApplicationError::reconcile(error.to_string()))?,
        )
    }
}

struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}
