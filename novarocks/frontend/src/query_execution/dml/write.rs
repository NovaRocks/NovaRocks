//! Sealed distributed-write dispatch for DML reverse ports.
//!
//! The binding retains the exact connector operation session created from the
//! admitted write template. It never reacquires a current connector generation.

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::contract::{DistributedQueryOutcome, DistributedQueryRequest};
use crate::query_execution::outcome::{QueryExecutionResult, WriteExecutionOutcome};
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::query_execution::service::QueryExecutionService;
use crate::query_execution::write_operation::ConnectorWriteOperationSession;

pub(crate) struct BoundDistributedWriteRequest {
    pub(crate) request: DistributedQueryRequest,
    pub(crate) session: ConnectorWriteOperationSession,
}

pub(crate) enum BoundDistributedWriteBinding {
    Bound(BoundDistributedWriteRequest),
    AbortRequired {
        session: ConnectorWriteOperationSession,
        reason: String,
    },
}

pub(crate) fn bind_prepared_distributed_write_request(
    query_execution: &QueryExecutionService,
    execution: &QueryExecutionContext,
    prepared: PreparedDistributedWriteRequest,
) -> Result<BoundDistributedWriteBinding, String> {
    let cohort_id = prepared.write_cohort_id();
    let session = query_execution
        .begin_write_operation(prepared.registration(), prepared.lease())
        .map_err(|error| error.to_string())?;
    let registration =
        match crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
            session.clone(),
            cohort_id,
        ) {
            Ok(registration) => registration,
            Err(error) => {
                return Ok(BoundDistributedWriteBinding::AbortRequired {
                    session,
                    reason: error.to_string(),
                });
            }
        };
    let request = match prepared.into_request(execution, registration) {
        Ok(request) => request,
        Err(error) => {
            return Ok(BoundDistributedWriteBinding::AbortRequired {
                session,
                reason: error.to_string(),
            });
        }
    };
    Ok(BoundDistributedWriteBinding::Bound(
        BoundDistributedWriteRequest { request, session },
    ))
}

pub(crate) fn execute_bound_distributed_write_request(
    query_execution: &QueryExecutionService,
    request: DistributedQueryRequest,
) -> Result<QueryExecutionResult, String> {
    let (query_result, write_commit, write_abort, connector_completion) = query_execution
        .execute(request)
        .and_then(DistributedQueryOutcome::into_write)
        .map(WriteExecutionOutcome::into_parts_with_connector)
        .map_err(|error| error.to_string())?;
    Ok(QueryExecutionResult {
        query_result,
        write_commit,
        write_abort,
        connector_completion,
        fragment_profiles: Vec::new(),
    })
}

pub(crate) fn scan_preparation_options(
    settings: &novarocks_sql::compiler::SessionOptimizerSettings,
    execution: &QueryExecutionContext,
) -> Result<crate::query_execution::preparation::ScanPreparationOptions, String> {
    let target_parallelism = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .or_else(|| {
            #[cfg(test)]
            {
                Some(std::num::NonZeroUsize::new(1).expect("one is non-zero"))
            }
            #[cfg(not(test))]
            {
                None
            }
        })
        .ok_or_else(|| {
            "connector split preparation requires a non-empty admitted backend topology".to_string()
        })?;
    Ok(
        crate::query_execution::preparation::ScanPreparationOptions::new(
            settings.connector_static_predicate_pushdown_enabled(),
            target_parallelism,
            None,
        ),
    )
}
