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

//! Terminal MySQL response encoding for governed Query Application statements.

use std::io;

use novarocks_query_application::cancellation::QueryCancellationReason;
use novarocks_query_application::protocol_delivery::GovernedProtocolOwner;
use novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome;
use novarocks_query_application::session_error::QueryServiceError;
use opensrv_mysql::{ErrorKind, InitWriter, OkResponse, QueryResultWriter};
use tokio::io::AsyncWrite;

use crate::{error_kind_for_domain_code, error_kind_for_query_service_error};

/// Maps a typed Query Application error to its MySQL wire kind.
pub fn mysql_error_kind(error: &QueryServiceError) -> ErrorKind {
    error
        .user_error()
        .and_then(|user_error| error_kind_for_domain_code(user_error.code().as_str()))
        .unwrap_or_else(|| error_kind_for_query_service_error(error.kind()))
}

/// Writes an ungoverned terminal OK response.
pub async fn write_terminal_ok<W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    write_terminal_ok_one(results)
        .await?
        .no_more_results()
        .await
}

/// Writes one terminal OK and returns the writer for a negotiated following
/// statement result.
pub async fn write_terminal_ok_one<'writer, W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'writer, W>,
) -> io::Result<QueryResultWriter<'writer, W>> {
    results.complete_one(OkResponse::default()).await
}

/// Writes the final OK response and settles its governed statement owner.
pub async fn write_governed_terminal_ok<W: AsyncWrite + Unpin>(
    protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match write_governed_terminal_ok_one(protocol, results).await? {
        crate::governed_result_writer::MysqlStatementWriteOutcome::Continue(results) => {
            results.no_more_results().await
        }
        crate::governed_result_writer::MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

pub async fn write_governed_terminal_ok_one<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<crate::governed_result_writer::MysqlStatementWriteOutcome<'writer, W>> {
    match protocol.seal_success_visibility() {
        GovernedStatementVisibilitySealOutcome::Sealed => {
            match results.complete_one(OkResponse::default()).await {
                Ok(results) => {
                    let _ = protocol.complete();
                    Ok(
                        crate::governed_result_writer::MysqlStatementWriteOutcome::Continue(
                            results,
                        ),
                    )
                }
                Err(error) => {
                    let _ = protocol.client_disconnected();
                    Err(error)
                }
            }
        }
        GovernedStatementVisibilitySealOutcome::Cancelled(_) => {
            let _ = protocol.settle_cancellation();
            results
                .error(
                    ErrorKind::ER_QUERY_INTERRUPTED,
                    b"query cancelled before terminal OK",
                )
                .await
                .map(|_| crate::governed_result_writer::MysqlStatementWriteOutcome::Terminated)
        }
        GovernedStatementVisibilitySealOutcome::Stale => {
            let _ = protocol.fail();
            results
                .error(
                    ErrorKind::ER_UNKNOWN_ERROR,
                    b"governed statement became stale before terminal OK",
                )
                .await
                .map(|_| crate::governed_result_writer::MysqlStatementWriteOutcome::Terminated)
        }
    }
}

/// Writes a COM_INIT_DB OK response and settles its governed statement owner.
pub async fn write_governed_init_ok<W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    writer: InitWriter<'_, W>,
) -> io::Result<()> {
    match protocol.seal_success_visibility() {
        GovernedStatementVisibilitySealOutcome::Sealed => match writer.ok().await {
            Ok(()) => {
                let _ = protocol.complete();
                Ok(())
            }
            Err(error) => {
                let _ = protocol.client_disconnected();
                Err(error)
            }
        },
        GovernedStatementVisibilitySealOutcome::Cancelled(_) => {
            let _ = protocol.settle_cancellation();
            writer
                .error(
                    ErrorKind::ER_QUERY_INTERRUPTED,
                    b"query cancelled before terminal OK",
                )
                .await
        }
        GovernedStatementVisibilitySealOutcome::Stale => {
            let _ = protocol.fail();
            writer
                .error(
                    ErrorKind::ER_UNKNOWN_ERROR,
                    b"governed statement became stale before terminal OK",
                )
                .await
        }
    }
}

/// Writes a COM_INIT_DB error response and settles its governed statement owner.
pub async fn write_governed_init_error<W: AsyncWrite + Unpin>(
    error: QueryServiceError,
    mut protocol: GovernedProtocolOwner,
    writer: InitWriter<'_, W>,
) -> io::Result<()> {
    if cancellation_overrides_typed_error(protocol.cancellation().reason()) {
        let _ = protocol.settle_cancellation();
        writer
            .error(ErrorKind::ER_QUERY_INTERRUPTED, b"query cancelled")
            .await
    } else {
        let _ = protocol.fail();
        writer
            .error(mysql_error_kind(&error), error.message().as_bytes())
            .await
    }
}

/// Writes the final error response and settles its governed statement owner.
pub async fn write_governed_terminal_error<W: AsyncWrite + Unpin>(
    error: QueryServiceError,
    mut protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    if cancellation_overrides_typed_error(protocol.cancellation().reason()) {
        let _ = protocol.settle_cancellation();
        return results
            .error(ErrorKind::ER_QUERY_INTERRUPTED, b"query cancelled")
            .await;
    }
    match results
        .error(mysql_error_kind(&error), error.message().as_bytes())
        .await
    {
        Ok(()) => {
            let _ = protocol.fail();
            Ok(())
        }
        Err(error) => {
            let _ = protocol.client_disconnected();
            Err(error)
        }
    }
}

fn cancellation_overrides_typed_error(reason: Option<QueryCancellationReason>) -> bool {
    !matches!(
        reason,
        Some(QueryCancellationReason::DeadlineExceeded { .. })
    ) && reason.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_user_error::{
        ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
    };

    #[test]
    fn typed_user_error_overrides_the_legacy_session_error_kind() {
        let user_error = UserError::from_descriptor(
            ErrorCodeDescriptor {
                code: ErrorCodeId::new("sql.analyze.unknown_table"),
                phase: ErrorPhase::Analyze,
                status: ErrorCodeStatus::Active,
            },
            "unknown table",
            None,
            RetryClass::Never,
        );

        assert_eq!(
            mysql_error_kind(&QueryServiceError::from_user_error(user_error)),
            ErrorKind::ER_NO_SUCH_TABLE
        );
    }

    #[test]
    fn deadline_cancellation_keeps_the_typed_timeout_error() {
        assert!(!cancellation_overrides_typed_error(Some(
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 1_000 },
        )));
        assert!(cancellation_overrides_typed_error(Some(
            QueryCancellationReason::ClientDisconnected,
        )));
    }
}
