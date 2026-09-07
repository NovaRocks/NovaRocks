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

//! Frontend-owned native execution adapter for current-process ANALYZE attempts.
//!
//! Core owns the provider-neutral statistics program and its one-shot
//! prepare/finish boundary.  This adapter owns the only native mapping step:
//! it encodes the sealed prepared view before returning the resulting request
//! to the carrier-neutral query-execution service.

use arrow::datatypes::FieldRef;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::backend_topology::BackendTopologyService;
use crate::query_execution::service::QueryExecutionService;
use crate::statistics_jobs::application::{
    StatisticsApplicationError, StatisticsAttemptExecutor, StatisticsAttemptRequest,
    StatisticsColumnIntent, rebind_table_object,
};
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorMutationOperationId, ConnectorRequestContext,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StatisticsCollectionStart, StatisticsCollectionStartRequest,
    StatisticsColumnSelection,
};

struct RequiredStatisticsExecution {
    table: novarocks_spi::connector::ConnectorTableHandle,
    data_version: novarocks_spi::connector::StatisticsDataVersion,
    read_version_ordinal: Option<i64>,
    required: Vec<novarocks_spi::connector::StatisticsRequiredAggregation>,
    session: Box<dyn novarocks_spi::connector::StatisticsCollectionSession>,
}

enum StatisticsExecutionDisposition {
    FinishedWithoutExecution,
    Required(RequiredStatisticsExecution),
}

/// Exact Frontend composition leaves retained by the process-owned ANALYZE worker.
/// Each collection takes a fresh live topology snapshot. The worker persists
/// only a logical target and physical object ID; a current binding, its data
/// version, and schema columns are attempt-local facts.
#[derive(Clone)]
pub(crate) struct StatisticsAttemptExecutionPorts {
    execution_role: novarocks_types::ClusterRole,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    /// The composition root's single typed control registry. A collection is
    /// an ordinary typed read, so it resolves its relation through the same
    /// installed generation every statement does.
    typed_connector_control: Arc<crate::connector::ConnectorControlHost>,
    backend_topology: BackendTopologyService,
    query_execution: QueryExecutionService,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    attempt_timeout: Duration,
}

impl StatisticsAttemptExecutionPorts {
    pub(crate) fn new(
        execution_role: novarocks_types::ClusterRole,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<crate::connector::ConnectorControlHost>,
        backend_topology: BackendTopologyService,
        query_execution: QueryExecutionService,
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
        attempt_timeout: Duration,
    ) -> Self {
        Self {
            execution_role,
            connector_control,
            typed_connector_control,
            backend_topology,
            query_execution,
            function_catalog,
            attempt_timeout,
        }
    }
}

/// Implements Core's process-worker port while retaining the native encoder
/// exclusively in Frontend.
pub(crate) struct FrontendStatisticsAttemptExecutor {
    ports: StatisticsAttemptExecutionPorts,
}

impl FrontendStatisticsAttemptExecutor {
    pub(crate) fn new(ports: StatisticsAttemptExecutionPorts) -> Self {
        Self { ports }
    }

    fn collection_context(
        &self,
        deadline: Instant,
        cancellation: crate::common::query_cancellation::QueryCancellationView,
    ) -> Result<ConnectorRequestContext, StatisticsApplicationError> {
        ConnectorRequestContext::try_new(
            deadline,
            Arc::new(AttemptCancellation(cancellation)),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn resolve_columns(
        request: &StatisticsAttemptRequest,
        bound_columns: &[FieldRef],
    ) -> Result<Vec<FieldRef>, StatisticsApplicationError> {
        match &request.columns {
            StatisticsColumnIntent::AllColumns => Ok(bound_columns.to_vec()),
            StatisticsColumnIntent::Explicit(requested_columns) => {
                let mut resolved = Vec::with_capacity(requested_columns.len());
                for requested in requested_columns {
                    let mut matches = bound_columns
                        .iter()
                        .filter(|bound| bound.name().eq_ignore_ascii_case(requested));
                    let Some(column) = matches.next() else {
                        return Err(StatisticsApplicationError::new(format!(
                            "ANALYZE requested column '{requested}' does not exist on the rebound table"
                        )));
                    };
                    if matches.next().is_some() {
                        return Err(StatisticsApplicationError::new(format!(
                            "ANALYZE requested column '{requested}' matches multiple rebound table columns"
                        )));
                    }
                    if resolved.iter().any(|existing: &FieldRef| {
                        existing.name().eq_ignore_ascii_case(column.name())
                    }) {
                        return Err(StatisticsApplicationError::new(format!(
                            "ANALYZE requested column '{requested}' is duplicated"
                        )));
                    }
                    resolved.push(column.clone());
                }
                Ok(resolved)
            }
        }
    }

    fn operation_id(request: &StatisticsAttemptRequest) -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes(request.operation_id.to_bytes())
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
            } => Err(StatisticsApplicationError::publication(
                crate::statistics_jobs::application::StatisticsPublicationTerminal::KnownCommittedFinalization,
                failure.to_string(),
            )),
            ExternalMutationOutcome::KnownUncommitted { failure } => Err(
                StatisticsApplicationError::publication(
                    crate::statistics_jobs::application::StatisticsPublicationTerminal::KnownUncommitted,
                    failure.to_string(),
                ),
            ),
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                Err(StatisticsApplicationError::publication(
                    crate::statistics_jobs::application::StatisticsPublicationTerminal::CommitUnknown,
                    failure.to_string(),
                ))
            }
        }
    }

    fn resolve_execution_disposition(
        start: StatisticsCollectionStart,
        cancellation: &crate::common::query_cancellation::QueryCancellationView,
    ) -> Result<StatisticsExecutionDisposition, StatisticsApplicationError> {
        let (table, data_version, read_version_ordinal, required, session) = start.into_parts();
        if required.is_empty() {
            if cancellation.is_cancelled() {
                return Err(StatisticsApplicationError::new(
                    "statistics attempt cancelled before provider session finish",
                ));
            }
            Self::outcome(
                session
                    .finish(Vec::new())
                    .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
            )?;
            return Ok(StatisticsExecutionDisposition::FinishedWithoutExecution);
        }
        Ok(StatisticsExecutionDisposition::Required(
            RequiredStatisticsExecution {
                table,
                data_version,
                read_version_ordinal,
                required,
                session,
            },
        ))
    }
}

impl StatisticsAttemptExecutor for FrontendStatisticsAttemptExecutor {
    fn execute(
        &self,
        request: &StatisticsAttemptRequest,
        cancellation: crate::common::query_cancellation::QueryCancellationView,
    ) -> Result<(), StatisticsApplicationError> {
        let attempt_deadline = Instant::now()
            .checked_add(self.ports.attempt_timeout)
            .ok_or_else(|| {
                StatisticsApplicationError::new("statistics attempt deadline overflow")
            })?;
        let context = self.collection_context(attempt_deadline, cancellation.clone())?;
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(&request.connector_instance_id)
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let planning_lease = self
            .ports
            .connector_control
            .acquire_current(&instance_id)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        // Rebinding and collection preparation intentionally share one current
        // connector generation. Do not translate this error: its typed physical
        // object binding classification must reach the process worker unchanged.
        let binding = rebind_table_object(&planning_lease, context.clone(), request)?;
        let selection = match &request.columns {
            StatisticsColumnIntent::AllColumns => StatisticsColumnSelection::Default,
            StatisticsColumnIntent::Explicit(_) => StatisticsColumnSelection::explicit(
                Self::resolve_columns(request, &binding.sql_columns)?
                    .into_iter()
                    .map(|column| Arc::<str>::from(column.name().as_str()))
                    .collect(),
            )
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        };
        let lease = planning_lease
            .derive_statistics_lease()
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let start = lease
            .begin_collection(StatisticsCollectionStartRequest {
                operation_id: Self::operation_id(request),
                table: binding.table.clone(),
                data_version: binding.data_version.clone(),
                selection,
                context: context.clone(),
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let StatisticsExecutionDisposition::Required(required_execution) =
            Self::resolve_execution_disposition(start, &cancellation)?
        else {
            return Ok(());
        };
        let RequiredStatisticsExecution {
            table,
            data_version,
            read_version_ordinal,
            required,
            session,
        } = required_execution;
        let program = crate::query_execution::statistics::StatisticsCollectionProgram::try_new(
            table,
            data_version,
            read_version_ordinal,
            required,
            crate::query_execution::statistics::StatisticsExecutionPolicy::try_new(
                crate::query_execution::statistics::StatisticsExecutionMode::ProcessJobAttempt,
                self.ports.attempt_timeout,
            )
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let topology = self
            .ports
            .backend_topology
            .snapshot()
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let execution = crate::common::admitted_query_context::QueryExecutionContext::new(
            self.ports.execution_role,
            topology,
            Some(attempt_deadline),
            cancellation.clone(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        );
        debug_assert_eq!(Some(context.deadline()), execution.deadline());

        // The attempt already names its relation; preparation must read that
        // one and no other, so the names travel with the program rather than
        // being recovered from a second catalog lookup.
        let relation = crate::query_execution::statistics::StatisticsRelationIdentity::try_new(
            request.connector_instance_id.as_str(),
            request.namespace.as_str(),
            request.table.as_str(),
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        // The sequence is intentional: Core prepares immutable provider facts;
        // Frontend maps the sealed view; Core consumes the exact attachment.
        let prepared = crate::query_execution::statistics::prepare_statistics_collection_request(
            crate::query_execution::statistics::StatisticsPlanningServices::new(
                self.ports.connector_control.as_ref(),
                &self.ports.typed_connector_control,
                self.ports.function_catalog.as_ref(),
            ),
            &execution,
            context.clone(),
            &relation,
            program,
            planning_lease,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let native_attachment = crate::native::fragment_encoder::encode_native_fragment_bundle(
            prepared.encoding_view(),
        )
        .map_err(StatisticsApplicationError::new)?;
        let distributed = prepared
            .finish(native_attachment)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let artifacts = self
            .ports
            .query_execution
            .execute(distributed)
            .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_statistics)
            .map(|outcome| outcome.into_artifacts())
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        if cancellation.is_cancelled() {
            return Err(StatisticsApplicationError::new(
                "statistics attempt cancelled before provider session finish",
            ));
        }
        Self::outcome(
            session
                .finish(artifacts)
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        )
    }
}

struct AttemptCancellation(crate::common::query_cancellation::QueryCancellationView);

impl novarocks_spi::connector::ConnectorCancellation for AttemptCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ConnectorTableHandle, ExternalMutationEffect, ProviderBindingEpoch,
        StatisticsArtifactDraft, StatisticsArtifactIdentity, StatisticsCollectionSession,
        StatisticsDataVersion, StatisticsEvidenceRevision, StatisticsReceipt,
    };

    use super::*;

    struct EmptyCollectionSession {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        data_version: StatisticsDataVersion,
        finish_calls: Arc<AtomicUsize>,
    }

    impl StatisticsCollectionSession for EmptyCollectionSession {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn operation_id(&self) -> ConnectorMutationOperationId {
            self.operation_id
        }

        fn expectations(&self) -> &[StatisticsArtifactIdentity] {
            &[]
        }

        fn finish(
            self: Box<Self>,
            artifacts: Vec<StatisticsArtifactDraft>,
        ) -> Result<
            ExternalMutationOutcome<StatisticsReceipt>,
            novarocks_spi::connector::ConnectorError,
        > {
            assert!(artifacts.is_empty());
            self.finish_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt: StatisticsReceipt::try_new(
                    self.descriptor.clone(),
                    self.incarnation,
                    self.operation_id,
                    self.data_version.clone(),
                    StatisticsEvidenceRevision::try_new(Bytes::from_static(b"empty"))?,
                    Bytes::new(),
                )?,
                finalization: ExternalMutationFinalization::Complete,
            })
        }
    }

    #[test]
    fn empty_collection_finishes_without_creating_distributed_tasks() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("ice.main").expect("instance ID"),
        };
        let incarnation = ProviderBindingEpoch::from_bytes([7; 16]);
        let operation_id = ConnectorMutationOperationId::from_bytes([8; 16]);
        let data_version = StatisticsDataVersion::try_new(Bytes::from_static(b"snapshot-42"))
            .expect("data version");
        let finish_calls = Arc::new(AtomicUsize::new(0));
        let start = StatisticsCollectionStart::try_new(
            ConnectorTableHandle::try_new(
                descriptor.instance_id.clone(),
                Bytes::from_static(b"orders"),
            )
            .expect("table handle"),
            data_version.clone(),
            None,
            Vec::new(),
            Box::new(EmptyCollectionSession {
                descriptor,
                incarnation,
                operation_id,
                data_version,
                finish_calls: Arc::clone(&finish_calls),
            }),
        )
        .expect("empty collection start");

        let cancellation = crate::common::query_cancellation::QueryCancellationSource::new();
        let disposition = FrontendStatisticsAttemptExecutor::resolve_execution_disposition(
            start,
            &cancellation.view(),
        )
        .expect("empty collection succeeds");
        let distributed_task_count = match disposition {
            StatisticsExecutionDisposition::FinishedWithoutExecution => 0,
            StatisticsExecutionDisposition::Required(_) => 1,
        };

        assert_eq!(distributed_task_count, 0);
        assert_eq!(finish_calls.load(Ordering::SeqCst), 1);
    }
}
