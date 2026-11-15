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

//! Typed frontend application contract for unified statistics commands.
//!
//! This module deliberately contains no parser AST or raw-SQL interception.
//! The frontend owns SQL/connector adaptation and target resolution. The
//! original statement owns capture, computation, publication and actual close.

use std::fmt;
use std::sync::Arc;

use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorControlPlanningPin, ConnectorControlRegistry,
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableObjectBindingFailure, ConnectorTableObjectCaptureRequest,
    ConnectorTableObjectSelector, ConnectorTableResolution, LakePublicationId,
    StatisticsDataVersion,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

/// Column-selection intent retained by the original ANALYZE statement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StatisticsColumnIntent {
    /// The attempt expands all SQL-visible columns from its captured schema.
    AllColumns,
    /// Explicit SQL column names supplied by the statement.
    Explicit(Vec<String>),
}

/// The first complete target capture, retained through collection and publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTargetCapture {
    pub connector_instance_id: String,
    pub namespace: String,
    pub table: String,
    pub object_id: Vec<u8>,
    pub binding: StatisticsAttemptBinding,
}

/// Identity and column intent retained by one original ANALYZE statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAttemptRequest {
    pub operation_id: LakePublicationId,
    pub connector_instance_id: String,
    pub namespace: String,
    pub table: String,
    pub object_id: Vec<u8>,
    pub columns: StatisticsColumnIntent,
}

/// The original captured binding, retained with its exact planning generation
/// through collection, publication and actual close.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAttemptBinding {
    pub table: novarocks_spi::connector::ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
    pub sql_columns: Vec<arrow::datatypes::FieldRef>,
}

/// The frontend captures the original ANALYZE target once.
pub trait StatisticsTargetResolver: Send + Sync {
    fn planning_pin(
        &self,
        _target: &StatisticsTableTarget,
    ) -> Result<ConnectorControlPlanningPin, StatisticsApplicationError> {
        Err(StatisticsApplicationError::new(
            "statistics capture has no governed planning pin",
        ))
    }
    fn capture_table_object_async<'a>(
        &'a self,
        _target: &'a StatisticsTableTarget,
        _context: ConnectorRequestContext,
        _lease: &'a ConnectorControlPlanningPin,
        _cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<StatisticsTargetCapture, StatisticsApplicationError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(StatisticsApplicationError::new(
                "statistics capture has no asynchronous source",
            ))
        })
    }
}

pub struct ConnectorStatisticsTargetResolver {
    controls: Arc<dyn ConnectorControlRegistry>,
}

impl ConnectorStatisticsTargetResolver {
    pub fn new(controls: Arc<dyn ConnectorControlRegistry>) -> Self {
        Self { controls }
    }
}

impl StatisticsTargetResolver for ConnectorStatisticsTargetResolver {
    fn planning_pin(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<ConnectorControlPlanningPin, StatisticsApplicationError> {
        let id = ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        self.controls
            .acquire_current(&id)
            .map_err(StatisticsApplicationError::from_connector_error)
    }
    fn capture_table_object_async<'a>(
        &'a self,
        target: &'a StatisticsTableTarget,
        context: ConnectorRequestContext,
        lease: &'a ConnectorControlPlanningPin,
        cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<StatisticsTargetCapture, StatisticsApplicationError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let instance_id = ConnectorInstanceId::parse(&target.catalog)
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
            if lease.binding().statistics().is_none() {
                return Err(StatisticsApplicationError::from_connector_error(
                    ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                        "connector control generation has no statistics capability",
                    ),
                ));
            }
            let captured = lease
                .binding()
                .metadata()
                .capture_table_object_binding_for_statistics_async(
                    ConnectorTableObjectCaptureRequest {
                        table: ConnectorTableIdentity {
                            instance_id,
                            namespace: Arc::from(target.namespace.as_str()),
                            table: Arc::from(target.table.as_str()),
                        },
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        selector: ConnectorTableObjectSelector::Current,
                        context: context.clone(),
                    },
                    cpu,
                )
                .await
                .map_err(|error| StatisticsApplicationError::from_target_error(error, target))?;
            lease
                .binding()
                .metadata()
                .admit_table_job(
                    novarocks_spi::connector::ConnectorTableJobAdmissionRequest {
                        table: ConnectorTableIdentity {
                            instance_id: ConnectorInstanceId::parse(&target.catalog).map_err(
                                |error| StatisticsApplicationError::new(error.to_string()),
                            )?,
                            namespace: Arc::from(target.namespace.as_str()),
                            table: Arc::from(target.table.as_str()),
                        },
                        job: novarocks_spi::connector::ConnectorTableJobKind::Statistics,
                        context: context.clone(),
                    },
                )
                .map_err(StatisticsApplicationError::from_connector_error)?;
            Ok(StatisticsTargetCapture {
                connector_instance_id: captured.metadata.table.owner().as_str().to_string(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                object_id: captured.object_id.as_bytes().to_vec(),
                binding: StatisticsAttemptBinding {
                    data_version: captured.metadata.statistics_data_version.clone().ok_or_else(|| StatisticsApplicationError::new("connector metadata did not provide a statistics data-version for captured table"))?,
                    sql_columns: sql_visible_columns(captured.metadata.schema()),
                    table: captured.metadata.table,
                },
            })
        })
    }
}

/// The SQL-visible fields, carrying their types.
///
/// A caller that only validates names takes them off the field. A caller that
/// builds a statistics metric request needs the type: which metrics a column
/// can answer depends on it.
fn sql_visible_columns(schema: &arrow::datatypes::Schema) -> Vec<arrow::datatypes::FieldRef> {
    schema
        .fields()
        .iter()
        .filter(|field| {
            field
                .metadata()
                .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
        })
        .map(Arc::clone)
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationCommand {
    AnalyzeTable {
        target: StatisticsTableTarget,
        columns: StatisticsColumnIntent,
    },
    ShowStats {
        target: StatisticsTableTarget,
    },
}
pub trait StatisticsApplicationPort: Send + Sync {
    fn prepare_statement_action(
        &self,
        command: StatisticsApplicationCommand,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        context: &ConnectorRequestContext,
        family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        StatisticsApplicationError,
    >;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsApplicationError {
    message: String,
    user_error: Option<novarocks_user_error::UserError>,
    target_binding_failure: Option<ConnectorTableObjectBindingFailure>,
    source: Option<ConnectorError>,
    statistics_failure: Option<super::publication::StatisticsFailure>,
    publication_failure: Option<super::publication::StatisticsPublicationFailure>,
    publication_terminal: Option<novarocks_spi::connector::LakePublicationTerminal>,
}

impl StatisticsApplicationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            user_error: None,
            target_binding_failure: None,
            source: None,
            statistics_failure: None,
            publication_failure: None,
            publication_terminal: None,
        }
    }

    pub(crate) fn from_connector_error(error: ConnectorError) -> Self {
        let target_binding_failure = error.table_object_binding_failure();
        Self {
            message: error.to_string(),
            user_error: None,
            target_binding_failure,
            source: Some(error),
            statistics_failure: None,
            publication_failure: None,
            publication_terminal: None,
        }
    }

    fn from_target_error(error: ConnectorError, target: &StatisticsTableTarget) -> Self {
        let mut projected = Self::from_connector_error(error);
        if projected.source.as_ref().is_some_and(|source| {
            source.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound
        }) {
            projected.user_error = Some(
                novarocks_sql::analyze_error::AnalyzeError::unknown_table(
                    format!(
                        "Table '{}.{}.{}' does not exist",
                        target.catalog, target.namespace, target.table
                    ),
                    novarocks_parser::Span::new(0, 0),
                )
                .to_user_error(None),
            );
        }
        projected
    }

    /// Preserve the original connector fact at synchronous command admission.
    pub(crate) fn into_command_error(self) -> novarocks_query_application::api::CommandError {
        use novarocks_query_application::api::{CommandError, CommandErrorKind};
        if let Some(user) = self.user_error {
            return CommandError::from_user_error(user);
        }
        match self.source {
            Some(source) => CommandError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(source),
            ),
            None => CommandError::new(CommandErrorKind::Rejected, self.message),
        }
    }

    pub(crate) fn from_statistics_attempt(
        publication_id: LakePublicationId,
        target: &StatisticsTableTarget,
        result: &Result<
            super::publication::StatisticsPublicationOutcome,
            super::publication::StatisticsAttemptError,
        >,
    ) -> Option<Self> {
        use super::publication::StatisticsPublicationOutcome as O;
        use novarocks_spi::connector::{
            LakePublicationDisposition as D, LakePublicationFamily, LakePublicationMarkerHeader,
            LakePublicationNextAction, LakePublicationTarget, LakePublicationTerminal,
        };
        let (failure, publication_failure, disposition) = match result {
            Ok(O::NoOp {
                finalization_failure: None,
                ..
            })
            | Ok(O::KnownCommitted {
                finalization_failure: None,
                ..
            }) => return None,
            Ok(O::NoOp {
                finalization_failure: Some(failure),
                ..
            }) => (None, Some(*failure), None),
            Ok(O::KnownCommitted {
                finalization_failure: Some(failure),
                ..
            }) => (None, Some(*failure), Some(D::KnownCommitted)),
            Ok(O::KnownUncommitted { failure, .. }) => {
                (None, Some(*failure), Some(D::KnownUncommitted))
            }
            Ok(O::CommitUnknown { failure }) => (None, Some(*failure), Some(D::CommitUnknown)),
            Err(error) => (Some(error.failure()), None, None),
        };
        let publication_terminal = disposition.map(|disposition| {
            LakePublicationTerminal::new(
                LakePublicationMarkerHeader::new(publication_id, LakePublicationFamily::Statistics),
                LakePublicationTarget::try_new(
                    target.catalog.clone(),
                    target.namespace.clone(),
                    Some(target.table.clone()),
                    None,
                )
                .expect("captured statistics target"),
                disposition,
                if disposition.do_not_retry() {
                    LakePublicationNextAction::InspectPublishedState
                } else {
                    LakePublicationNextAction::RetryStatement
                },
                None,
            )
        });
        Some(Self {
            message: publication_failure
                .as_ref()
                .map(|failure| failure.public_message())
                .or_else(|| failure.as_ref().map(|f| f.error.message().to_string()))
                .unwrap_or_else(|| "statistics statement failed".into()),
            user_error: None,
            target_binding_failure: None,
            source: None,
            statistics_failure: failure,
            publication_failure,
            publication_terminal,
        })
    }

    pub(super) fn invalid_analyze_property(message: impl Into<String>) -> Self {
        let user = novarocks_sql::analyze_error::AnalyzeError::invalid_analyze_property(message)
            .to_user_error(None);
        Self {
            message: user.message().to_string(),
            user_error: Some(user),
            ..Self::new("")
        }
    }

    pub(crate) fn to_query_service_error(
        &self,
    ) -> novarocks_query_application::session_error::QueryServiceError {
        use novarocks_query_application::{
            execution_error::{ExecutionFailureInput, ExecutionFailureSite},
            session_error::{QueryServiceError, QueryServiceErrorKind},
        };
        let mut error = if let Some(failure) = &self.publication_failure {
            QueryServiceError::from_execution_failure(ExecutionFailureInput::new(
                failure.fact(),
                novarocks_execution_contract::SafeDetail::new(failure.public_message())
                    .expect("fixed statistics publication detail fits safe boundary"),
                ExecutionFailureSite::Coordinator,
            ))
            .with_compile_control_provenance(failure.compile_control())
        } else if let Some(failure) = &self.statistics_failure {
            failure.error.clone()
        } else if let (Some(user), Some(source)) = (&self.user_error, &self.source) {
            QueryServiceError::from_user_error_with_cause(
                user.clone(),
                novarocks_failure_contract::SharedFailureCause::new(source.clone()),
            )
        } else if let Some(source) = &self.source {
            QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(source.clone()),
            )
        } else if let Some(user) = &self.user_error {
            QueryServiceError::from_user_error(user.clone())
        } else {
            QueryServiceError::new(QueryServiceErrorKind::Internal, &self.message)
        };
        if let Some(terminal) = &self.publication_terminal {
            if error.execution_failure().is_none() && terminal.do_not_retry() {
                let header = terminal.header();
                let disposition = match terminal.disposition() {
                    novarocks_spi::connector::LakePublicationDisposition::KnownCommitted => {
                        "KnownCommitted"
                    }
                    novarocks_spi::connector::LakePublicationDisposition::CommitUnknown => {
                        "CommitUnknown"
                    }
                    novarocks_spi::connector::LakePublicationDisposition::KnownUncommitted => {
                        unreachable!()
                    }
                };
                let message = format!(
                    "{} (lake publication id={} family={} disposition={} do_not_retry=true; do not retry statement)",
                    error.message(),
                    header.publication_id(),
                    header.family().as_str(),
                    disposition
                );
                error = QueryServiceError::with_publication_terminal(message, terminal.clone())
                    .with_compile_control(error.compile_control_error())
                    .with_compile_control_provenance(error.compile_control_provenance());
            } else {
                error = error.attach_publication_terminal(terminal.clone());
            }
        }
        error
    }

    pub const fn target_binding_failure(&self) -> Option<ConnectorTableObjectBindingFailure> {
        self.target_binding_failure
    }
}

impl fmt::Display for StatisticsApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsApplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::super::publication::{
        StatisticsPublicationFailure, StatisticsPublicationOutcome as O,
    };
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    fn target() -> StatisticsTableTarget {
        StatisticsTableTarget {
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "t".into(),
        }
    }
    fn failed() -> StatisticsPublicationFailure {
        let original = novarocks_type_contract::CompileControlError::ResourceExhausted;
        StatisticsPublicationFailure::capture(
            original.failure_fact(),
            Some(original.provenance()),
            &"private Puffin path and credential",
        )
    }
    #[test]
    fn no_op_and_committed_success_do_not_manufacture_statement_failures() {
        for outcome in [
            O::NoOp {
                finalization_failure: None,
                cleanup_failure: Some(failed()),
            },
            O::KnownCommitted {
                finalization_failure: None,
                cleanup_failure: Some(failed()),
            },
        ] {
            assert!(
                StatisticsApplicationError::from_statistics_attempt(
                    LakePublicationId::new_v7(),
                    &target(),
                    &Ok(outcome)
                )
                .is_none()
            );
        }
    }
    #[test]
    fn publication_errors_keep_no_op_committed_unknown_and_retry_dispositions_distinct() {
        use novarocks_spi::connector::LakePublicationDisposition as D;
        for (outcome, disposition) in [
            (
                O::NoOp {
                    finalization_failure: Some(failed()),
                    cleanup_failure: None,
                },
                None,
            ),
            (
                O::KnownCommitted {
                    finalization_failure: Some(failed()),
                    cleanup_failure: None,
                },
                Some(D::KnownCommitted),
            ),
            (
                O::KnownUncommitted {
                    failure: failed(),
                    cleanup_failure: None,
                },
                Some(D::KnownUncommitted),
            ),
            (
                O::CommitUnknown { failure: failed() },
                Some(D::CommitUnknown),
            ),
        ] {
            let id = LakePublicationId::new_v7();
            let error =
                StatisticsApplicationError::from_statistics_attempt(id, &target(), &Ok(outcome))
                    .unwrap()
                    .to_query_service_error();
            assert_eq!(
                error
                    .publication_terminal()
                    .map(|terminal| terminal.disposition()),
                disposition
            );
            if let Some(terminal) = error.publication_terminal() {
                assert_eq!(terminal.header().publication_id(), id);
                assert_eq!(terminal.do_not_retry(), disposition.unwrap().do_not_retry());
            }
            assert_eq!(error.execution_failure().unwrap().fact(), failed().fact());
            assert!(!error.message().contains("private Puffin"));
            assert!(error.compile_control_provenance().is_some());
        }
    }
    #[test]
    fn missing_analyze_target_preserves_both_table_not_found_and_original_cause() {
        let original = ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::NotFound,
            "opaque original miss",
        );
        let error = StatisticsApplicationError::from_target_error(original.clone(), &target())
            .to_query_service_error();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.analyze.unknown_table"
        );
        assert_eq!(
            error
                .failure_cause()
                .unwrap()
                .downcast_ref::<ConnectorError>()
                .unwrap(),
            &original
        );
        assert!(error.message().contains("ice.db.t"));
        let unsupported = ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "unsupported relation",
        );
        assert!(
            StatisticsApplicationError::from_target_error(unsupported, &target())
                .to_query_service_error()
                .execution_failure()
                .is_some()
        );
    }
    #[test]
    fn connector_source_survives_statement_error_projection() {
        let original = ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "source relation is unsupported",
        );
        let error = StatisticsApplicationError::from_connector_error(original.clone())
            .to_query_service_error();
        assert_eq!(
            error.execution_failure().unwrap().fact(),
            original.failure_fact()
        );
        assert_eq!(
            error
                .failure_cause()
                .unwrap()
                .downcast_ref::<ConnectorError>()
                .unwrap(),
            &original
        );
    }
}
