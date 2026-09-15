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
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use novarocks_statistics_application::{
    StatisticsAttemptError as CoreStatisticsAttemptError,
    StatisticsAttemptExecutor as CoreStatisticsAttemptExecutor, StatisticsColumns,
    StatisticsFailure, StatisticsJob, StatisticsPublicationFact, StatisticsPublicationOutcome,
};

use crate::query_execution::service::QueryExecutionService;
use crate::statistics_jobs::application::{
    StatisticsApplicationError, StatisticsAttemptRequest, StatisticsColumnIntent,
    rebind_table_object,
};
use novarocks_query_application::api::BackendTopologyService;
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorMutationOperationId, ConnectorRequestContext,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StatisticsCollectionStartRequest, StatisticsColumnSelection,
};

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
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    backend_topology: BackendTopologyService,
    query_execution: QueryExecutionService,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    attempt_timeout: Duration,
}

impl StatisticsAttemptExecutionPorts {
    pub(crate) fn new(
        execution_role: novarocks_types::ClusterRole,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
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
                if resolved
                    .iter()
                    .any(|existing: &FieldRef| existing.name().eq_ignore_ascii_case(column.name()))
                {
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

/// The only state allowed to cross the collection/publication phase boundary.
/// It retains the provider session and the exact artifacts that session must
/// publish; no catalog handle, planning lease, or query runtime escapes the
/// completed collection phase.
struct PendingThreePhaseStatisticsAttempt {
    session: Box<dyn novarocks_spi::connector::StatisticsCollectionSession>,
    request: Option<crate::query_execution::contract::DistributedQueryRequest>,
    artifacts: Option<Vec<novarocks_spi::connector::StatisticsArtifactDraft>>,
}

fn empty_collection_pending(
    session: Box<dyn novarocks_spi::connector::StatisticsCollectionSession>,
) -> PendingThreePhaseStatisticsAttempt {
    PendingThreePhaseStatisticsAttempt {
        session,
        request: None,
        // A provider session, including an empty collection, crosses only into
        // publish. This keeps closure and the authoritative publication fact
        // in the final phase.
        artifacts: Some(Vec::new()),
    }
}

/// T12 adapter from the statistics product worker to the real FE query
/// execution service. Its phases make the retained provider publication
/// session explicit and cannot expose an unfinished query execution across
/// `collect`.
pub(crate) struct FrontendThreePhaseStatisticsAttemptExecutor {
    ports: StatisticsAttemptExecutionPorts,
    pending: Mutex<
        HashMap<
            novarocks_statistics_application::StatisticsJobId,
            PendingThreePhaseStatisticsAttempt,
        >,
    >,
}

impl FrontendThreePhaseStatisticsAttemptExecutor {
    pub(crate) fn new(ports: StatisticsAttemptExecutionPorts) -> Self {
        Self {
            ports,
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn failure(message: impl Into<String>) -> CoreStatisticsAttemptError {
        CoreStatisticsAttemptError::Failed(StatisticsFailure {
            message: Arc::from(message.into()),
        })
    }

    fn scope_error(error: novarocks_workload_control::WorkError) -> CoreStatisticsAttemptError {
        if matches!(error, novarocks_workload_control::WorkError::Cancelled(_)) {
            CoreStatisticsAttemptError::Cancelled(StatisticsFailure {
                message: Arc::from(error.to_string()),
            })
        } else {
            Self::failure(error.to_string())
        }
    }

    fn application_error(error: StatisticsApplicationError) -> CoreStatisticsAttemptError {
        if error.target_binding_failure().is_some() {
            CoreStatisticsAttemptError::Stale(StatisticsFailure {
                message: Arc::from(error.to_string()),
            })
        } else if error.publication_terminal().is_some() {
            Self::failure(error.to_string())
        } else {
            Self::failure(error.to_string())
        }
    }

    fn request(
        job: &StatisticsJob,
    ) -> Result<StatisticsAttemptRequest, CoreStatisticsAttemptError> {
        let operation_id = novarocks_spi::connector::LakePublicationId::try_from_uuid(
            job.publication_id.as_uuid(),
        )
        .map_err(|error| Self::failure(error.to_string()))?;
        Ok(StatisticsAttemptRequest {
            operation_id,
            connector_instance_id: job.target.catalog.to_string(),
            namespace: job.target.namespace.to_string(),
            table: job.target.table.to_string(),
            object_id: job.target.object_id.to_vec(),
            columns: match &job.columns {
                StatisticsColumns::All => StatisticsColumnIntent::AllColumns,
                StatisticsColumns::Explicit(columns) => StatisticsColumnIntent::Explicit(
                    columns.iter().map(ToString::to_string).collect(),
                ),
            },
        })
    }

    fn context(
        &self,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<
        (
            Instant,
            novarocks_query_application::cancellation::QueryCancellationView,
        ),
        CoreStatisticsAttemptError,
    > {
        scope.check().map_err(Self::scope_error)?;
        let deadline = scope
            .cancellation()
            .map_err(Self::scope_error)?
            .deadline()
            .map(Into::into)
            .unwrap_or_else(|| Instant::now() + self.ports.attempt_timeout);
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationView::governed(
                scope.cancellation().map_err(Self::scope_error)?,
                None,
            );
        if cancellation.is_cancelled() {
            return Err(CoreStatisticsAttemptError::Cancelled(StatisticsFailure {
                message: Arc::from("statistics attempt cancelled before phase start"),
            }));
        }
        Ok((deadline, cancellation))
    }

    fn collection_context(
        &self,
        deadline: Instant,
        cancellation: novarocks_query_application::cancellation::QueryCancellationView,
    ) -> Result<ConnectorRequestContext, StatisticsApplicationError> {
        ConnectorRequestContext::try_new(
            deadline,
            Arc::new(AttemptCancellation(cancellation)),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn publication_outcome(
        outcome: ExternalMutationOutcome<novarocks_spi::connector::StatisticsReceipt>,
    ) -> StatisticsPublicationOutcome {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { finalization, .. } => {
                StatisticsPublicationOutcome {
                    fact: StatisticsPublicationFact::KnownCommitted,
                    finalization_failure: match finalization {
                        ExternalMutationFinalization::Complete => None,
                        ExternalMutationFinalization::Failed(error) => Some(StatisticsFailure {
                            message: Arc::from(error.to_string()),
                        }),
                    },
                }
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => StatisticsPublicationOutcome {
                fact: StatisticsPublicationFact::KnownUncommitted,
                finalization_failure: None,
            },
            ExternalMutationOutcome::CommitUnknown { .. } => StatisticsPublicationOutcome {
                fact: StatisticsPublicationFact::CommitUnknown,
                finalization_failure: None,
            },
        }
    }

    fn abort_pending(
        &self,
        job_id: novarocks_statistics_application::StatisticsJobId,
        original: CoreStatisticsAttemptError,
    ) -> CoreStatisticsAttemptError {
        let pending = match self.pending.lock() {
            Ok(mut attempts) => attempts.remove(&job_id),
            Err(_) => {
                return Self::with_abort_context(
                    original,
                    "statistics phase state lock poisoned while aborting provider session",
                );
            }
        };
        match pending {
            Some(pending) => Self::abort_pending_session(pending, original),
            None => original,
        }
    }

    fn abort_pending_session(
        pending: PendingThreePhaseStatisticsAttempt,
        original: CoreStatisticsAttemptError,
    ) -> CoreStatisticsAttemptError {
        match pending.session.abort() {
            Ok(()) => original,
            Err(error) => Self::with_abort_context(
                original,
                format!("statistics provider session abort failed: {error}"),
            ),
        }
    }

    fn with_abort_context(
        original: CoreStatisticsAttemptError,
        context: impl AsRef<str>,
    ) -> CoreStatisticsAttemptError {
        let context = context.as_ref();
        match original {
            CoreStatisticsAttemptError::Failed(failure) => {
                CoreStatisticsAttemptError::Failed(StatisticsFailure {
                    message: Arc::from(format!("{}; {context}", failure.message)),
                })
            }
            CoreStatisticsAttemptError::Stale(failure) => {
                CoreStatisticsAttemptError::Stale(StatisticsFailure {
                    message: Arc::from(format!("{}; {context}", failure.message)),
                })
            }
            CoreStatisticsAttemptError::Cancelled(failure) => {
                CoreStatisticsAttemptError::Cancelled(StatisticsFailure {
                    message: Arc::from(format!("{}; {context}", failure.message)),
                })
            }
        }
    }
}

impl CoreStatisticsAttemptExecutor for FrontendThreePhaseStatisticsAttemptExecutor {
    fn prepare(
        &self,
        job: &StatisticsJob,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<(), CoreStatisticsAttemptError> {
        let request = Self::request(job)?;
        let (deadline, cancellation) = self.context(scope)?;
        let context = self
            .collection_context(deadline, cancellation.clone())
            .map_err(Self::application_error)?;
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(&request.connector_instance_id)
                .map_err(|error| Self::failure(error.to_string()))?;
        let planning_lease = self
            .ports
            .connector_control
            .acquire_current(&instance_id)
            .map_err(|error| Self::failure(error.to_string()))?;
        let binding = rebind_table_object(&planning_lease, context.clone(), &request)
            .map_err(Self::application_error)?;
        let selection = match &request.columns {
            StatisticsColumnIntent::AllColumns => StatisticsColumnSelection::Default,
            StatisticsColumnIntent::Explicit(_) => StatisticsColumnSelection::explicit(
                resolve_columns(&request, &binding.sql_columns)
                    .map_err(Self::application_error)?
                    .into_iter()
                    .map(|column| Arc::<str>::from(column.name().as_str()))
                    .collect(),
            )
            .map_err(|error| Self::failure(error.to_string()))?,
        };
        let lease = planning_lease
            .derive_statistics_lease()
            .map_err(|error| Self::failure(error.to_string()))?;
        let start = lease
            .begin_collection(StatisticsCollectionStartRequest {
                operation_id: operation_id(&request),
                table: binding.table.clone(),
                data_version: binding.data_version.clone(),
                selection,
                context: context.clone(),
            })
            .map_err(|error| Self::failure(error.to_string()))?;
        let (table, data_version, read_version_ordinal, required, session) = start.into_parts();
        let pending = if required.is_empty() {
            empty_collection_pending(session)
        } else {
            let request = (|| {
                let program = crate::query_execution::statistics::StatisticsCollectionProgram::try_new(
                    table,
                    data_version,
                    read_version_ordinal,
                    required,
                    crate::query_execution::statistics::StatisticsExecutionPolicy::try_new(
                        crate::query_execution::statistics::StatisticsExecutionMode::BackgroundCollectionAttempt,
                        self.ports.attempt_timeout,
                    )
                    .map_err(|error| Self::failure(error.to_string()))?,
                )
                .map_err(|error| Self::failure(error.to_string()))?;
                let topology = self
                    .ports
                    .backend_topology
                    .snapshot()
                    .map_err(|error| Self::failure(error.to_string()))?;
                let execution =
                    novarocks_query_application::admitted_query_context::QueryExecutionContext::new(
                        self.ports.execution_role,
                        topology,
                        Some(deadline),
                        cancellation,
                        novarocks_sql::compiler::SessionOptimizerSettings::default(),
                    );
                let relation =
                    crate::query_execution::statistics::StatisticsRelationIdentity::try_new(
                        request.connector_instance_id.as_str(),
                        request.namespace.as_str(),
                        request.table.as_str(),
                    )
                    .map_err(|error| Self::failure(error.to_string()))?;
                let prepared =
                    crate::query_execution::statistics::prepare_statistics_collection_request(
                        crate::query_execution::statistics::StatisticsPlanningServices::new(
                            self.ports.connector_control.as_ref(),
                            &self.ports.typed_connector_control,
                            self.ports.function_catalog.as_ref(),
                        ),
                        &execution,
                        context,
                        &relation,
                        program,
                        planning_lease,
                    )
                    .map_err(|error| Self::failure(error.to_string()))?;
                let native = crate::native::fragment_encoder::encode_native_fragment_bundle(
                    prepared.encoding_view(),
                )
                .map_err(Self::failure)?;
                prepared
                    .finish(native)
                    .map_err(|error| Self::failure(error.to_string()))
            })();
            let request = match request {
                Ok(request) => request,
                Err(error) => {
                    return Err(Self::abort_pending_session(
                        PendingThreePhaseStatisticsAttempt {
                            session,
                            request: None,
                            artifacts: None,
                        },
                        error,
                    ));
                }
            };
            PendingThreePhaseStatisticsAttempt {
                session,
                request: Some(request),
                artifacts: None,
            }
        };
        let mut attempts = self
            .pending
            .lock()
            .map_err(|_| Self::failure("statistics phase state lock poisoned"))?;
        if attempts.contains_key(&job.id) {
            drop(attempts);
            let error = Self::failure("statistics attempt already prepared");
            return Err(Self::abort_pending_session(pending, error));
        }
        attempts.insert(job.id, pending);
        Ok(())
    }

    fn collect(
        &self,
        job: &StatisticsJob,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<(), CoreStatisticsAttemptError> {
        if let Err(error) = scope.check().map_err(Self::scope_error) {
            return Err(self.abort_pending(job.id, error));
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| Self::failure("statistics phase state lock poisoned"))?
            .remove(&job.id)
            .ok_or_else(|| Self::failure("statistics collection has no prepared attempt"))?;
        if pending.request.is_none() {
            if pending.artifacts.is_none() {
                let error =
                    Self::failure("statistics collection has neither request nor artifacts");
                return Err(Self::abort_pending_session(pending, error));
            }
            self.pending
                .lock()
                .map_err(|_| Self::failure("statistics phase state lock poisoned"))?
                .insert(job.id, pending);
            return Ok(());
        }
        let request = pending
            .request
            .take()
            .ok_or_else(|| Self::failure("statistics collection already executed"))?;
        let artifacts = match self
            .ports
            .query_execution
            .execute(request)
            .and_then(crate::query_execution::outcome::DistributedQueryOutcome::into_statistics)
            .map(|outcome| outcome.into_artifacts())
        {
            Ok(artifacts) => artifacts,
            Err(error) => {
                let error = Self::failure(error.to_string());
                return Err(Self::abort_pending_session(pending, error));
            }
        };
        pending.artifacts = Some(artifacts);
        self.pending
            .lock()
            .map_err(|_| Self::failure("statistics phase state lock poisoned"))?
            .insert(job.id, pending);
        Ok(())
    }

    fn publish(
        &self,
        job: &StatisticsJob,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<StatisticsPublicationOutcome, CoreStatisticsAttemptError> {
        if let Err(error) = scope.check().map_err(Self::scope_error) {
            return Err(self.abort_pending(job.id, error));
        }
        let pending = self
            .pending
            .lock()
            .map_err(|_| Self::failure("statistics phase state lock poisoned"))?
            .remove(&job.id)
            .ok_or_else(|| Self::failure("statistics publication has no collected attempt"))?;
        let Some(artifacts) = pending.artifacts else {
            let error = Self::failure("statistics publication requires collected artifacts");
            return Err(Self::abort_pending_session(pending, error));
        };
        pending
            .session
            .finish(artifacts)
            .map(Self::publication_outcome)
            .map_err(|error| Self::failure(error.to_string()))
    }
}

struct AttemptCancellation(novarocks_query_application::cancellation::QueryCancellationView);

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
        ExternalMutationEffect, ProviderBindingEpoch, StatisticsArtifactDraft,
        StatisticsArtifactIdentity, StatisticsCollectionSession, StatisticsDataVersion,
        StatisticsEvidenceRevision, StatisticsReceipt,
    };

    use super::*;

    struct EmptyCollectionSession {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        data_version: StatisticsDataVersion,
        finish_calls: Arc<AtomicUsize>,
        abort_calls: Arc<AtomicUsize>,
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

        fn abort(self: Box<Self>) -> Result<(), novarocks_spi::connector::ConnectorError> {
            self.abort_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn empty_collection_is_retained_until_publication_without_a_query_request() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("ice.main").expect("instance ID"),
        };
        let incarnation = ProviderBindingEpoch::from_bytes([7; 16]);
        let operation_id = ConnectorMutationOperationId::from_bytes([8; 16]);
        let data_version = StatisticsDataVersion::try_new(Bytes::from_static(b"snapshot-42"))
            .expect("data version");
        let finish_calls = Arc::new(AtomicUsize::new(0));
        let abort_calls = Arc::new(AtomicUsize::new(0));
        let pending = empty_collection_pending(Box::new(EmptyCollectionSession {
            descriptor,
            incarnation,
            operation_id,
            data_version,
            finish_calls: Arc::clone(&finish_calls),
            abort_calls: Arc::clone(&abort_calls),
        }));
        let PendingThreePhaseStatisticsAttempt {
            session,
            request,
            artifacts,
        } = pending;

        assert!(request.is_none());
        assert_eq!(artifacts.as_ref().map(Vec::len), Some(0));
        assert_eq!(finish_calls.load(Ordering::SeqCst), 0);
        let outcome = session
            .finish(artifacts.expect("empty artifacts"))
            .expect("empty collection publishes");
        assert!(matches!(
            FrontendThreePhaseStatisticsAttemptExecutor::publication_outcome(outcome).fact,
            StatisticsPublicationFact::KnownCommitted
        ));
        assert_eq!(finish_calls.load(Ordering::SeqCst), 1);
        assert_eq!(abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn abandoning_a_prepared_collection_consumes_the_provider_session_once() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("ice.main").expect("instance ID"),
        };
        let abort_calls = Arc::new(AtomicUsize::new(0));
        let pending = empty_collection_pending(Box::new(EmptyCollectionSession {
            descriptor,
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
            operation_id: ConnectorMutationOperationId::from_bytes([8; 16]),
            data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"snapshot-42"))
                .expect("data version"),
            finish_calls: Arc::new(AtomicUsize::new(0)),
            abort_calls: Arc::clone(&abort_calls),
        }));

        let error = FrontendThreePhaseStatisticsAttemptExecutor::abort_pending_session(
            pending,
            FrontendThreePhaseStatisticsAttemptExecutor::failure("collection failed"),
        );

        assert!(matches!(error, CoreStatisticsAttemptError::Failed(_)));
        assert_eq!(abort_calls.load(Ordering::SeqCst), 1);
    }
}
