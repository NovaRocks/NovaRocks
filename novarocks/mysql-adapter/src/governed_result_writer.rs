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

//! Governed Query Application result delivery over the MySQL wire.

use std::io;

use arrow::array::{
    Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    MapArray, StringArray, StructArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_query_application::api::{
    QueryExecutionError, QueryExecutionErrorKind, QueryResult, ResultDelivery, ResultFailureView,
    ResultField as QueryResultColumn, ResultSchema, decoded_result_batch_governance_charge,
};
use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationView};
use novarocks_query_application::protocol_delivery::{
    GovernedImmediateStatementResult, ImmediateResultBatch, StreamingStatementResult,
};
use novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome;
use novarocks_types::FieldRenderSchema;
use novarocks_workload_control::{
    LocalResourceAuthority, Reservation, ResourceClass, WorkError, WorkScope,
};
use opensrv_mysql::{Column, ErrorKind, QueryResultWriter, U24_MAX};
use tokio::io::AsyncWrite;

const MYSQL_TERMINAL_PROTOCOL_BYTES_UPPER_BOUND: u64 = 64;

enum ProtocolWriteFailure {
    Cancelled(QueryExecutionError),
    Native(QueryExecutionError),
    Encoding(io::Error),
    Io(io::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProtocolWriteSettlement {
    Cancellation,
    ProtocolFailed,
    ClientDisconnected,
}

pub enum MysqlStatementWriteOutcome<'writer, W: AsyncWrite + Unpin> {
    Continue(QueryResultWriter<'writer, W>),
    Terminated,
}

impl ProtocolWriteFailure {
    fn settlement(&self) -> ProtocolWriteSettlement {
        match self {
            Self::Cancelled(_) => ProtocolWriteSettlement::Cancellation,
            Self::Native(_) | Self::Encoding(_) => ProtocolWriteSettlement::ProtocolFailed,
            Self::Io(_) => ProtocolWriteSettlement::ClientDisconnected,
        }
    }
}

/// Delivers an already-materialized immediate Query Application result.
///
/// This result has no governed delivery owner, but the MySQL adapter still
/// owns its schema, rows, and terminal wire transitions.
pub async fn write_query_result<W: AsyncWrite + Unpin>(
    result: QueryResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    write_query_result_one(result, results)
        .await?
        .no_more_results()
        .await
}

/// Writes one materialized result and returns the writer for the next
/// negotiated result on this exact connection.
pub async fn write_query_result_one<'writer, W: AsyncWrite + Unpin>(
    result: QueryResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<QueryResultWriter<'writer, W>> {
    let batches = result.batches.iter().collect::<Vec<_>>();
    crate::write_record_batches_one(&result.columns, &batches, results).await
}

pub async fn write_governed_query_result<W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match write_governed_query_result_one(result, results).await? {
        MysqlStatementWriteOutcome::Continue(results) => results.no_more_results().await,
        MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

pub async fn write_governed_query_result_one<'writer, W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let (result, mut protocol) = result.into_parts();
    let schema_bytes = match mysql_query_result_schema_protocol_bytes_upper_bound(&result.columns) {
        Ok(bytes) => bytes,
        Err(message) => {
            let error = invalid_query_result_delivery(message);
            let _ = protocol.fail();
            let message = error.to_string().into_bytes();
            return results
                .error(ErrorKind::ER_UNKNOWN_ERROR, &message)
                .await
                .map(|_| MysqlStatementWriteOutcome::Terminated);
        }
    };
    let cancellation = protocol.cancellation();
    let (resources, scope) = protocol.reservation_inputs();
    let reserve = reserve_data_when_available(resources, scope, schema_bytes);
    tokio::pin!(reserve);
    let schema_reservation = match tokio::select! {
        biased;
        reason = cancellation.cancelled() => {
            Err(cancelled_query_result_delivery(reason))
        }
        reservation = &mut reserve => reservation.map_err(|error| {
            failed_query_result_delivery(format!("reserve MySQL result schema bytes: {error}"))
        })
    } {
        Ok(reservation) => reservation,
        Err(error) => {
            if error.kind() == QueryExecutionErrorKind::Cancelled {
                let _ = protocol.settle_cancellation();
            } else {
                let _ = protocol.fail();
            }
            let message = error.to_string().into_bytes();
            return results
                .error(ErrorKind::ER_UNKNOWN_ERROR, &message)
                .await
                .map(|_| MysqlStatementWriteOutcome::Terminated);
        }
    };
    let mysql_columns = match crate::mysql_columns_for_result_fields(&result.columns) {
        Ok(columns) => columns,
        Err(error) => {
            let error = invalid_query_result_delivery(error.to_string());
            let _ = protocol.fail();
            let message = error.to_string().into_bytes();
            return results
                .error(ErrorKind::ER_UNKNOWN_ERROR, &message)
                .await
                .map(|_| MysqlStatementWriteOutcome::Terminated);
        }
    };
    let cancellation = protocol.cancellation();
    let mut writer = match crate::start_cancellable_result(
        results,
        mysql_columns.as_slice(),
        cancellation,
    )
    .await
    {
        Ok(writer) => writer,
        Err(crate::MysqlResultStartError::Cancelled(error)) => {
            let _ = protocol.settle_cancellation();
            return Err(interrupted_error(error.to_string()));
        }
        Err(crate::MysqlResultStartError::Io(error)) => {
            let _ = protocol.client_disconnected();
            return Err(error);
        }
    };
    drop(schema_reservation);

    for raw_batch in result.batches {
        let decoded_bytes = match decoded_result_batch_governance_charge(&raw_batch) {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = protocol.fail();
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        let cancellation = protocol.cancellation();
        let (resources, scope) = protocol.reservation_inputs();
        let reserve_fetch = resources.reserve_result_credit_when_available(&scope, decoded_bytes);
        tokio::pin!(reserve_fetch);
        let credit = match tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                Err(cancelled_query_result_delivery(reason))
            }
            credit = &mut reserve_fetch => credit.map_err(|error| {
                failed_query_result_delivery(format!("reserve immediate result fetch bytes: {error}"))
            })
        } {
            Ok(credit) => credit,
            Err(error) => {
                if error.kind() == QueryExecutionErrorKind::Cancelled {
                    let _ = protocol.settle_cancellation();
                } else {
                    let _ = protocol.fail();
                }
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        // The result batch is already materialized by the synchronous command
        // executor. Account it through the normal fetch/decode transitions
        // before handing that exact Arrow backing to the protocol writer.
        let credit = match credit.begin_fetch() {
            Ok(credit) => credit,
            Err(error) => {
                let error = failed_query_result_delivery(format!(
                    "begin immediate result credit fetch: {error}"
                ));
                let _ = protocol.fail();
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        let credit = match credit.retain_raw(decoded_bytes) {
            Ok(credit) => credit,
            Err(error) => {
                let (error, _credit) = error.into_parts();
                let error =
                    failed_query_result_delivery(format!("retain immediate result bytes: {error}"));
                let _ = protocol.fail();
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        let cancellation = protocol.cancellation();
        let resources = protocol.reservation_inputs().0;
        let reserve_decode = credit.reserve_decode_when_available(&resources, decoded_bytes);
        tokio::pin!(reserve_decode);
        let credit = match tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                Err(cancelled_query_result_delivery(reason))
            }
            credit = &mut reserve_decode => credit.map_err(|error| {
                let (error, _credit) = error.into_parts();
                failed_query_result_delivery(format!("reserve immediate result decode bytes: {error}"))
            })
        } {
            Ok(credit) => credit,
            Err(error) => {
                if error.kind() == QueryExecutionErrorKind::Cancelled {
                    let _ = protocol.settle_cancellation();
                } else {
                    let _ = protocol.fail();
                }
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        let credit = match credit.queue_decoded(decoded_bytes) {
            Ok(credit) => credit,
            Err(error) => {
                let (error, _credit) = error.into_parts();
                let error = failed_query_result_delivery(format!(
                    "queue immediate decoded result bytes: {error}"
                ));
                let _ = protocol.fail();
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        let batch = match ImmediateResultBatch::try_new(raw_batch, credit) {
            Ok(batch) => batch,
            Err(error) => {
                let _ = protocol.fail();
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        let protocol_bytes =
            match mysql_text_batch_protocol_bytes_upper_bound(batch.batch(), &result.columns) {
                Ok(bytes) => bytes,
                Err(message) => {
                    let error = invalid_query_result_delivery(message);
                    let _ = protocol.fail();
                    return finish_stream_error_terminated(
                        writer,
                        ErrorKind::ER_UNKNOWN_ERROR,
                        &error,
                    )
                    .await;
                }
            };
        let cancellation = protocol.cancellation();
        let resources = protocol.reservation_inputs().0;
        let reserve = batch.reserve_protocol_when_available(&resources, protocol_bytes);
        tokio::pin!(reserve);
        let batch = match tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                Err(cancelled_query_result_delivery(reason))
            }
            batch = &mut reserve => batch.map_err(|error| {
                failed_query_result_delivery(format!("reserve MySQL result bytes: {}", error.error()))
            })
        } {
            Ok(batch) => batch,
            Err(error) => {
                if error.kind() == QueryExecutionErrorKind::Cancelled {
                    let _ = protocol.settle_cancellation();
                } else {
                    let _ = protocol.fail();
                }
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        let batch = match batch.begin_protocol_write(protocol_bytes) {
            Ok(batch) => batch,
            Err(error) => {
                let _ = protocol.fail();
                return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                    .await;
            }
        };
        if let Err(error) = write_governed_batch(
            &mut writer,
            batch.batch(),
            &result.columns,
            protocol.cancellation(),
        )
        .await
        {
            let settlement = error.settlement();
            match error {
                ProtocolWriteFailure::Cancelled(error) => {
                    debug_assert_eq!(settlement, ProtocolWriteSettlement::Cancellation);
                    batch.fail();
                    let _ = protocol.settle_cancellation();
                    return Err(interrupted_error(error.to_string()));
                }
                ProtocolWriteFailure::Encoding(error) => {
                    debug_assert_eq!(settlement, ProtocolWriteSettlement::ProtocolFailed);
                    batch.fail();
                    let _ = protocol.fail();
                    return Err(error);
                }
                ProtocolWriteFailure::Io(error) => {
                    debug_assert_eq!(settlement, ProtocolWriteSettlement::ClientDisconnected);
                    // An I/O error, including `Interrupted`, is evidence about
                    // the socket only. Cancellation is settled exclusively by
                    // the cancellation branch above.
                    batch.fail();
                    let _ = protocol.client_disconnected();
                    return Err(error);
                }
                ProtocolWriteFailure::Native(_) => {
                    unreachable!("immediate results have no native failure view")
                }
            }
        }
        if let Err(error) = batch.complete() {
            let _ = protocol.fail();
            return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                .await;
        }
    }

    let cancellation = protocol.cancellation();
    let (resources, scope) = protocol.reservation_inputs();
    let reserve = reserve_terminal_protocol_when_available(resources, scope);
    tokio::pin!(reserve);
    let terminal_reservation = match tokio::select! {
        biased;
        reason = cancellation.cancelled() => Err(cancelled_query_result_delivery(reason)),
        reservation = &mut reserve => reservation.map_err(|error| {
            failed_query_result_delivery(format!("reserve MySQL result EOF bytes: {error}"))
        }),
    } {
        Ok(reservation) => reservation,
        Err(error) => {
            if error.kind() == QueryExecutionErrorKind::Cancelled {
                let _ = protocol.settle_cancellation();
            } else {
                let _ = protocol.fail();
            }
            return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                .await;
        }
    };
    match protocol.seal_success_visibility() {
        GovernedStatementVisibilitySealOutcome::Sealed => {}
        GovernedStatementVisibilitySealOutcome::Cancelled(reason) => {
            let error = governed_cancelled_query_result_delivery(reason);
            let _ = protocol.settle_cancellation();
            return finish_stream_error_terminated(writer, ErrorKind::ER_QUERY_INTERRUPTED, &error)
                .await;
        }
        GovernedStatementVisibilitySealOutcome::Stale => {
            let error = failed_query_result_delivery(
                "governed query lost its statement generation before success visibility",
            );
            let _ = protocol.fail();
            return finish_stream_error_terminated(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                .await;
        }
    }
    match crate::finish_result_one(writer).await {
        Ok(writer) => {
            drop(terminal_reservation);
            let _ = protocol.complete();
            Ok(MysqlStatementWriteOutcome::Continue(writer))
        }
        Err(error) => {
            drop(terminal_reservation);
            let _ = protocol.client_disconnected();
            Err(error)
        }
    }
}

/// Writes one Query Application result without detaching its delivery and
/// resource owners from the protocol operation which consumes them.
pub async fn write_streaming_query_result<W: AsyncWrite + Unpin>(
    result: StreamingStatementResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match write_streaming_query_result_one(result, results).await? {
        MysqlStatementWriteOutcome::Continue(results) => results.no_more_results().await,
        MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

pub async fn write_streaming_query_result_one<'writer, W: AsyncWrite + Unpin>(
    mut result: StreamingStatementResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let schema_delivery = match result.begin_schema() {
        Some(delivery) => delivery,
        None => {
            let _ = result.fail();
            return Err(invalid_data_error(
                "streaming query result has no schema delivery".to_string(),
            ));
        }
    };
    let mut failure = match result.failure_view() {
        Some(failure) => failure,
        None => {
            let error =
                invalid_query_result_delivery("streaming query result has no failure observation");
            schema_delivery.fail(error.clone());
            let _ = result.fail();
            return Err(invalid_data_error(error.to_string()));
        }
    };
    let schema_bytes =
        match mysql_result_schema_protocol_bytes_upper_bound(schema_delivery.schema()) {
            Ok(bytes) => bytes,
            Err(message) => {
                let error = invalid_query_result_delivery(message);
                schema_delivery.fail(error.clone());
                let _ = result.fail();
                return Err(invalid_data_error(error.to_string()));
            }
        };
    let schema_reservation = {
        let cancellation = result.cancellation();
        let (resources, scope) = result.reservation_inputs();
        let reservation = reserve_data_when_available(resources, scope, schema_bytes);
        tokio::pin!(reservation);
        tokio::select! {
            biased;
            error = failure.wait() => {
                schema_delivery.fail(error.clone());
                let _ = result.fail();
                return results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                    .await
                    .map(|_| MysqlStatementWriteOutcome::Terminated);
            }
            reason = cancellation.cancelled() => {
                let error = cancelled_query_result_delivery(reason);
                schema_delivery.fail(error.clone());
                let _ = result.settle_cancellation();
                return results
                    .error(ErrorKind::ER_QUERY_INTERRUPTED, error.to_string().as_bytes())
                    .await
                    .map(|_| MysqlStatementWriteOutcome::Terminated);
            }
            reservation = &mut reservation => match reservation {
                Ok(reservation) => reservation,
                Err(error) => {
                    let error = failed_query_result_delivery(format!(
                        "reserve MySQL result schema bytes: {error}"
                    ));
                    schema_delivery.fail(error.clone());
                    let _ = result.fail();
                    return Err(invalid_data_error(error.to_string()));
                }
            }
        }
    };
    let columns = result_schema_to_query_result_columns(schema_delivery.schema());
    let mysql_columns = match crate::mysql_columns_for_result_fields(&columns) {
        Ok(columns) => columns,
        Err(error) => {
            let error = invalid_query_result_delivery(error.to_string());
            schema_delivery.fail(error.clone());
            let _ = result.fail();
            return Err(invalid_data_error(error.to_string()));
        }
    };
    let cancellation = result.cancellation();
    let mut writer = match crate::start_streaming_result(
        results,
        mysql_columns.as_slice(),
        cancellation,
        failure.clone(),
    )
    .await
    {
        Ok(writer) => writer,
        Err(crate::MysqlBatchWriteError::Native(error)) => {
            schema_delivery.fail(error.clone());
            let _ = result.fail();
            return Err(invalid_data_error(error.to_string()));
        }
        Err(crate::MysqlBatchWriteError::Cancelled(error)) => {
            schema_delivery.fail(error.clone());
            let _ = result.settle_cancellation();
            return Err(interrupted_error(error.to_string()));
        }
        Err(crate::MysqlBatchWriteError::Encoding(error)) => {
            let error = invalid_query_result_delivery(error.to_string());
            schema_delivery.fail(error.clone());
            let _ = result.fail();
            return Err(invalid_data_error(error.to_string()));
        }
        Err(crate::MysqlBatchWriteError::Io(error)) => {
            schema_delivery.fail(failed_query_result_delivery(format!(
                "write MySQL result schema: {error}"
            )));
            let _ = result.client_disconnected();
            return Err(error);
        }
    };
    schema_delivery.complete();
    drop(schema_reservation);

    loop {
        let next = {
            let cancellation = result.cancellation();
            let next = result.next_delivery();
            tokio::pin!(next);
            tokio::select! {
                biased;
                error = failure.wait() => Err(error),
                reason = cancellation.cancelled() => {
                    Err(cancelled_query_result_delivery(reason))
                }
                delivery = &mut next => delivery.and_then(|delivery| {
                    delivery.ok_or_else(|| failed_query_result_delivery(
                        "streaming query result ended without an acknowledged success EOF",
                    ))
                })
            }
        };
        let delivery = match next {
            Ok(delivery) => delivery,
            Err(error) => {
                let error = normalize_terminal_cancellation(error);
                let kind = if is_terminal_cancellation(&error) {
                    ErrorKind::ER_QUERY_INTERRUPTED
                } else {
                    ErrorKind::ER_UNKNOWN_ERROR
                };
                if is_terminal_cancellation(&error) {
                    let _ = result.settle_cancellation();
                } else {
                    let _ = result.fail();
                }
                return finish_stream_error_terminated(writer, kind, &error).await;
            }
        };

        match delivery {
            ResultDelivery::Batch(delivery) => {
                let batch = delivery.batch().clone();
                let protocol_bytes =
                    match mysql_text_batch_protocol_bytes_upper_bound(&batch, &columns) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            let error = invalid_query_result_delivery(error);
                            delivery.fail(error.clone());
                            let _ = result.fail();
                            return finish_stream_error_terminated(
                                writer,
                                ErrorKind::ER_UNKNOWN_ERROR,
                                &error,
                            )
                            .await;
                        }
                    };
                let resources = result.resources().clone();
                let reserved = 'reservation: {
                    let cancellation = result.cancellation();
                    let reserve =
                        delivery.reserve_protocol_when_available(&resources, protocol_bytes);
                    tokio::pin!(reserve);
                    let interrupt = tokio::select! {
                        biased;
                        error = failure.wait() => Some(error),
                        reason = cancellation.cancelled() => {
                            Some(cancelled_query_result_delivery(reason))
                        }
                        reserved = &mut reserve => break 'reservation match reserved {
                            Ok(delivery) => Ok(delivery),
                            Err(rejection) => {
                                let (error, delivery) = rejection.into_parts();
                                let error = failed_query_result_delivery(
                                    format!("reserve MySQL result bytes: {error}"),
                                );
                                delivery.fail(error.clone());
                                Err(error)
                            }
                        },
                    };
                    let error =
                        interrupt.expect("protocol reservation select returns an interrupt");
                    // Cancellation of the exact execution wakes its workload-backed
                    // reservation. Await it to recover the move-only delivery and
                    // report Failed instead of silently dropping its receipt.
                    let _ = result.request_cancel();
                    let delivery = match reserve.await {
                        Ok(delivery) => delivery,
                        Err(rejection) => rejection.into_parts().1,
                    };
                    delivery.fail(error.clone());
                    Err(error)
                };
                let delivery = match reserved {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        let error = normalize_terminal_cancellation(error);
                        let kind = if is_terminal_cancellation(&error) {
                            ErrorKind::ER_QUERY_INTERRUPTED
                        } else {
                            ErrorKind::ER_UNKNOWN_ERROR
                        };
                        if is_terminal_cancellation(&error) {
                            let _ = result.settle_cancellation();
                        } else {
                            let _ = result.fail();
                        }
                        return finish_stream_error_terminated(writer, kind, &error).await;
                    }
                };
                let delivery = match delivery.begin_protocol_write(protocol_bytes) {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        let _ = result.fail();
                        return finish_stream_error_terminated(
                            writer,
                            ErrorKind::ER_UNKNOWN_ERROR,
                            &error,
                        )
                        .await;
                    }
                };
                let cancellation = result.cancellation();
                if let Err(error) = write_streaming_batch(
                    &mut writer,
                    &batch,
                    &columns,
                    cancellation,
                    failure.clone(),
                )
                .await
                {
                    let settlement = error.settlement();
                    match error {
                        ProtocolWriteFailure::Cancelled(error) => {
                            debug_assert_eq!(settlement, ProtocolWriteSettlement::Cancellation);
                            delivery.fail(error.clone());
                            let _ = result.settle_cancellation();
                            return Err(interrupted_error(error.to_string()));
                        }
                        ProtocolWriteFailure::Native(error) => {
                            debug_assert_eq!(settlement, ProtocolWriteSettlement::ProtocolFailed);
                            delivery.fail(error.clone());
                            let _ = result.fail();
                            return Err(invalid_data_error(error.to_string()));
                        }
                        ProtocolWriteFailure::Encoding(error) => {
                            debug_assert_eq!(settlement, ProtocolWriteSettlement::ProtocolFailed);
                            delivery.fail(failed_query_result_delivery(format!(
                                "encode MySQL result batch: {error}"
                            )));
                            let _ = result.fail();
                            return Err(error);
                        }
                        ProtocolWriteFailure::Io(error) => {
                            debug_assert_eq!(
                                settlement,
                                ProtocolWriteSettlement::ClientDisconnected
                            );
                            delivery.fail(failed_query_result_delivery(format!(
                                "write MySQL result batch: {error}"
                            )));
                            let _ = result.client_disconnected();
                            return Err(error);
                        }
                    }
                }
                if let Err(error) = delivery.complete() {
                    let _ = result.fail();
                    return finish_stream_error_terminated(
                        writer,
                        ErrorKind::ER_UNKNOWN_ERROR,
                        &error,
                    )
                    .await;
                }
            }
            ResultDelivery::End(delivery) => {
                let cancellation = result.cancellation();
                let (resources, scope) = result.reservation_inputs();
                let reserve = reserve_terminal_protocol_when_available(resources, scope);
                tokio::pin!(reserve);
                let terminal_reservation = match tokio::select! {
                    biased;
                    error = failure.wait() => Err(error),
                    reason = cancellation.cancelled() => {
                        Err(cancelled_query_result_delivery(reason))
                    }
                    reservation = &mut reserve => reservation.map_err(|error| {
                        failed_query_result_delivery(format!(
                            "reserve MySQL result EOF bytes: {error}"
                        ))
                    }),
                } {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        let error = normalize_terminal_cancellation(error);
                        let kind = if is_terminal_cancellation(&error) {
                            ErrorKind::ER_QUERY_INTERRUPTED
                        } else {
                            ErrorKind::ER_UNKNOWN_ERROR
                        };
                        delivery.fail(error.clone());
                        if is_terminal_cancellation(&error) {
                            let _ = result.settle_cancellation();
                        } else {
                            let _ = result.fail();
                        }
                        return finish_stream_error_terminated(writer, kind, &error).await;
                    }
                };
                match result.seal_success_visibility() {
                    GovernedStatementVisibilitySealOutcome::Sealed => {}
                    GovernedStatementVisibilitySealOutcome::Cancelled(reason) => {
                        let error = governed_cancelled_query_result_delivery(reason);
                        delivery.fail(error.clone());
                        let _ = result.settle_cancellation();
                        return finish_stream_error_terminated(
                            writer,
                            ErrorKind::ER_QUERY_INTERRUPTED,
                            &error,
                        )
                        .await;
                    }
                    GovernedStatementVisibilitySealOutcome::Stale => {
                        let error = failed_query_result_delivery(
                            "governed query lost its statement generation before success visibility",
                        );
                        delivery.fail(error.clone());
                        let _ = result.fail();
                        return finish_stream_error_terminated(
                            writer,
                            ErrorKind::ER_UNKNOWN_ERROR,
                            &error,
                        )
                        .await;
                    }
                }
                let finished = crate::finish_streaming_result_one(writer, failure.clone()).await;
                match finished {
                    Ok(writer) => {
                        drop(terminal_reservation);
                        delivery.complete();
                        let _ = result.complete();
                        return Ok(MysqlStatementWriteOutcome::Continue(writer));
                    }
                    Err(crate::MysqlResultFinishError::Native(error)) => {
                        drop(terminal_reservation);
                        delivery.fail(error.clone());
                        let _ = result.fail();
                        return Err(invalid_data_error(error.to_string()));
                    }
                    Err(crate::MysqlResultFinishError::Io(error)) => {
                        drop(terminal_reservation);
                        delivery.fail(failed_query_result_delivery(format!(
                            "write MySQL success EOF: {error}"
                        )));
                        let _ = result.client_disconnected();
                        return Err(error);
                    }
                }
            }
        }
    }
}

async fn finish_stream_error<W: AsyncWrite + Unpin>(
    writer: opensrv_mysql::RowWriter<'_, '_, W>,
    kind: ErrorKind,
    error: &QueryExecutionError,
) -> io::Result<()> {
    crate::finish_result_error(writer, kind, error).await
}

async fn finish_stream_error_terminated<'writer, W: AsyncWrite + Unpin>(
    writer: opensrv_mysql::RowWriter<'writer, '_, W>,
    kind: ErrorKind,
    error: &QueryExecutionError,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    finish_stream_error(writer, kind, error)
        .await
        .map(|_| MysqlStatementWriteOutcome::Terminated)
}

async fn reserve_data_when_available(
    resources: LocalResourceAuthority,
    scope: WorkScope,
    bytes: u64,
) -> Result<Reservation, WorkError> {
    reserve_when_available(resources, scope, bytes, ResourceClass::Data).await
}

async fn reserve_terminal_protocol_when_available(
    resources: LocalResourceAuthority,
    scope: WorkScope,
) -> Result<Reservation, WorkError> {
    reserve_when_available(
        resources,
        scope,
        MYSQL_TERMINAL_PROTOCOL_BYTES_UPPER_BOUND,
        ResourceClass::Control,
    )
    .await
}

async fn reserve_when_available(
    resources: LocalResourceAuthority,
    scope: WorkScope,
    bytes: u64,
    class: ResourceClass,
) -> Result<Reservation, WorkError> {
    loop {
        match resources.reserve(&scope, bytes, class) {
            Ok(reservation) => return Ok(reservation),
            Err(WorkError::Capacity(_)) => {
                resources.wait_for_capacity(&scope, bytes, class).await?;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn write_streaming_batch<W: AsyncWrite + Unpin>(
    writer: &mut opensrv_mysql::RowWriter<'_, '_, W>,
    batch: &RecordBatch,
    columns: &[QueryResultColumn],
    cancellation: QueryCancellationView,
    failure: ResultFailureView,
) -> Result<(), ProtocolWriteFailure> {
    crate::write_streaming_batch(writer, batch, columns, cancellation, failure)
        .await
        .map_err(|error| match error {
            crate::MysqlBatchWriteError::Cancelled(error) => ProtocolWriteFailure::Cancelled(error),
            crate::MysqlBatchWriteError::Native(error) => ProtocolWriteFailure::Native(error),
            crate::MysqlBatchWriteError::Encoding(error) => ProtocolWriteFailure::Encoding(error),
            crate::MysqlBatchWriteError::Io(error) => ProtocolWriteFailure::Io(error),
        })
}

async fn write_governed_batch<W: AsyncWrite + Unpin>(
    writer: &mut opensrv_mysql::RowWriter<'_, '_, W>,
    batch: &RecordBatch,
    columns: &[QueryResultColumn],
    cancellation: QueryCancellationView,
) -> Result<(), ProtocolWriteFailure> {
    crate::write_cancellable_batch(writer, batch, columns, cancellation)
        .await
        .map_err(|error| match error {
            crate::MysqlBatchWriteError::Cancelled(error) => ProtocolWriteFailure::Cancelled(error),
            crate::MysqlBatchWriteError::Native(error) => ProtocolWriteFailure::Native(error),
            crate::MysqlBatchWriteError::Encoding(error) => ProtocolWriteFailure::Encoding(error),
            crate::MysqlBatchWriteError::Io(error) => ProtocolWriteFailure::Io(error),
        })
}

fn result_schema_to_query_result_columns(schema: &ResultSchema) -> Vec<QueryResultColumn> {
    schema.fields().to_vec()
}

fn mysql_result_schema_protocol_bytes_upper_bound(schema: &ResultSchema) -> Result<u64, String> {
    mysql_schema_protocol_bytes_upper_bound(
        schema.fields().iter().map(|field| field.name()),
        schema.fields().len(),
    )
}

fn mysql_query_result_schema_protocol_bytes_upper_bound(
    columns: &[QueryResultColumn],
) -> Result<u64, String> {
    mysql_schema_protocol_bytes_upper_bound(
        columns.iter().map(QueryResultColumn::name),
        columns.len(),
    )
}

fn mysql_schema_protocol_bytes_upper_bound<'a>(
    names: impl Iterator<Item = &'a str>,
    column_count: usize,
) -> Result<u64, String> {
    let mut retained_names = 0_u64;
    let mut largest_packet = 13_u64;
    for name in names {
        let bytes = usize_to_u64(name.len())?;
        retained_names = checked_wire_add(retained_names, bytes)?;
        let encoded_name = mysql_lenenc_string_upper_bound(bytes)?;
        largest_packet = largest_packet.max(checked_wire_add(encoded_name, 25)?);
    }
    let structs = usize_to_u64(column_count)?
        .checked_mul(
            u64::try_from(std::mem::size_of::<QueryResultColumn>() + std::mem::size_of::<Column>())
                .map_err(|_| "MySQL schema carrier size does not fit u64".to_string())?,
        )
        .ok_or_else(|| "MySQL schema carrier byte count overflow".to_string())?;
    // The adapter retains two owned name copies while opensrv writes one
    // column-definition packet at a time. PacketWriter may allocate a second
    // remaining-payload buffer after a partial vectored write, so two copies
    // of the largest packet are charged. Fixed carrier storage and one EOF /
    // count packet are included in the same pre-allocation reservation.
    checked_wire_add(
        checked_wire_add(
            retained_names
                .checked_mul(2)
                .ok_or_else(|| "MySQL schema name byte count overflow".to_string())?,
            structs,
        )?,
        checked_wire_add(
            largest_packet
                .checked_mul(2)
                .ok_or_else(|| "MySQL schema packet byte count overflow".to_string())?
                .max(18),
            64,
        )?,
    )
}

/// Returns a wire-size upper bound for the largest row packet retained while
/// one batch is written.
///
/// This walks Arrow offsets and logical container structure without rendering
/// values. In particular, JSON/list/map/struct strings are allocated exactly
/// once later, after the returned capacity has been reserved. Fixed-width
/// values use their protocol maxima and variable-width values use their Arrow
/// slice lengths plus the MySQL length prefix. The four-byte framing charge is
/// added once per 24-bit protocol packet.
fn mysql_text_batch_protocol_bytes_upper_bound(
    batch: &RecordBatch,
    columns: &[QueryResultColumn],
) -> Result<u64, String> {
    let mut largest = 0_u64;
    if batch.num_columns() != columns.len() || batch.schema().fields().len() != columns.len() {
        return Err("query result columns do not match Arrow batch".to_string());
    }
    for row in 0..batch.num_rows() {
        let mut payload = 0_u64;
        for ((column, field), declared) in batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .zip(columns)
        {
            let schema = FieldRenderSchema::from_field(field.as_ref());
            payload = checked_wire_add(
                payload,
                mysql_text_cell_upper_bound(column, declared, row, &schema)?,
            )?;
        }
        let packets = payload
            .checked_add(u64::try_from(U24_MAX).unwrap() - 1)
            .and_then(|bytes| bytes.checked_div(u64::try_from(U24_MAX).unwrap()))
            .ok_or_else(|| "MySQL protocol row byte count overflow".to_string())?
            .max(1);
        let wire = payload
            .checked_add(
                packets
                    .checked_mul(4)
                    .ok_or_else(|| "MySQL protocol header byte count overflow".to_string())?,
            )
            .ok_or_else(|| "MySQL protocol row byte count overflow".to_string())?;
        // opensrv_mysql keeps the complete packet payload and, after a
        // partial vectored write, may allocate one remaining-payload copy.
        // Charging twice the complete wire size also covers the temporary
        // rendered container while it is copied into PacketWriter.
        let retained = wire
            .checked_mul(2)
            .ok_or_else(|| "MySQL retained protocol row byte count overflow".to_string())?;
        largest = largest.max(retained);
    }
    Ok(largest.max(1))
}

fn mysql_text_cell_upper_bound(
    column: &ArrayRef,
    declared: &QueryResultColumn,
    row: usize,
    schema: &FieldRenderSchema,
) -> Result<u64, String> {
    if column.is_null(row) || matches!(column.data_type(), DataType::Null) {
        return Ok(1);
    }
    if declared
        .logical_type()
        .is_some_and(|logical| matches!(logical, novarocks_types::schema::SqlType::Decimal { .. }))
    {
        return mysql_lenenc_string_upper_bound(48);
    }
    if matches!(declared.data_type(), DataType::Date32)
        && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return mysql_lenenc_string_upper_bound(10);
    }
    if matches!(
        declared.data_type(),
        DataType::Time32(_) | DataType::Time64(_)
    ) && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return mysql_lenenc_string_upper_bound(32);
    }
    if schema.renders_opaque_binary()
        || (matches!(column.data_type(), DataType::Binary | DataType::LargeBinary)
            && is_opaque_aggregate_column(declared.name()))
    {
        return Ok(1);
    }

    let payload = match column.data_type() {
        DataType::Boolean => 2,
        DataType::Int8 => 5,
        DataType::Int16 => 7,
        DataType::Int32 => 12,
        DataType::Int64 => 21,
        DataType::UInt8 => 4,
        DataType::UInt16 => 6,
        DataType::UInt32 => 11,
        DataType::UInt64 => 21,
        DataType::Float32 => 25,
        DataType::Float64 => 33,
        DataType::Utf8 => {
            let value = downcast_array::<StringArray>(column, "StringArray")?.value(row);
            mysql_lenenc_string_upper_bound(usize_to_u64(value.len())?)?
        }
        DataType::LargeUtf8 => {
            let value = downcast_array::<LargeStringArray>(column, "LargeStringArray")?.value(row);
            mysql_lenenc_string_upper_bound(usize_to_u64(value.len())?)?
        }
        DataType::Binary => {
            let value = downcast_array::<BinaryArray>(column, "BinaryArray")?.value(row);
            mysql_lenenc_string_upper_bound(usize_to_u64(value.len())?)?
        }
        DataType::LargeBinary => {
            let value = downcast_array::<LargeBinaryArray>(column, "LargeBinaryArray")?.value(row);
            mysql_lenenc_string_upper_bound(usize_to_u64(value.len())?)?
        }
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            mysql_lenenc_string_upper_bound(40)?
        }
        DataType::Date32 => mysql_lenenc_string_upper_bound(10)?,
        DataType::Decimal128(precision, _) => {
            mysql_lenenc_string_upper_bound(u64::from(*precision) + 2)?
        }
        DataType::Time32(_) | DataType::Time64(_) => mysql_lenenc_string_upper_bound(32)?,
        DataType::Timestamp(_, _) => mysql_lenenc_string_upper_bound(32)?,
        DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            mysql_lenenc_string_upper_bound(mysql_container_upper_bound(
                column,
                row,
                Some(schema),
                false,
            )?)?
        }
        other => {
            return Err(format!(
                "standalone mysql server does not support output column type {other:?}"
            ));
        }
    };
    Ok(payload)
}

fn mysql_container_upper_bound(
    column: &ArrayRef,
    row: usize,
    schema: Option<&FieldRenderSchema>,
    json_string: bool,
) -> Result<u64, String> {
    if column.is_null(row) || matches!(column.data_type(), DataType::Null) {
        return Ok(4);
    }
    match column.data_type() {
        DataType::Boolean => Ok(1),
        DataType::Int8 => Ok(4),
        DataType::Int16 => Ok(6),
        DataType::Int32 => Ok(11),
        DataType::Int64 => Ok(20),
        DataType::Float32 => Ok(24),
        DataType::Float64 => Ok(32),
        DataType::Utf8 => {
            let bytes = usize_to_u64(
                downcast_array::<StringArray>(column, "StringArray")?
                    .value(row)
                    .len(),
            )?;
            quoted_string_upper_bound(bytes, if json_string { 6 } else { 2 })
        }
        DataType::Date32 => Ok(12),
        DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_, _) => Ok(66),
        DataType::Decimal128(precision, _) => Ok(u64::from(*precision) + 2),
        DataType::Decimal256(precision, _) => Ok(u64::from(*precision) + 2),
        DataType::Binary => {
            let bytes = usize_to_u64(
                downcast_array::<BinaryArray>(column, "BinaryArray")?
                    .value(row)
                    .len(),
            )?;
            quoted_string_upper_bound(bytes, 4)
        }
        DataType::FixedSizeBinary(width) => {
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH {
                Ok(40)
            } else {
                quoted_string_upper_bound(
                    u64::try_from(*width).map_err(|_| {
                        "negative fixed-size binary width in MySQL result".to_string()
                    })?,
                    4,
                )
            }
        }
        DataType::LargeBinary => {
            let bytes = usize_to_u64(
                downcast_array::<LargeBinaryArray>(column, "LargeBinaryArray")?
                    .value(row)
                    .len(),
            )?;
            // Variant contains no compression and is size-limited. A dictionary
            // key can be referenced O(n) times, so rendered JSON is bounded by
            // O(n^2); a JSON-valued child may then escape every rendered byte.
            let side = checked_wire_add(bytes, 1)?;
            let expanded = side
                .checked_mul(side)
                .and_then(|value| value.checked_mul(if json_string { 48 } else { 8 }))
                .ok_or_else(|| "MySQL Variant upper bound overflow".to_string())?;
            checked_wire_add(expanded, 512)
        }
        DataType::List(field) => {
            let array = downcast_array::<ListArray>(column, "ListArray")?;
            let offsets = array.value_offsets();
            let start = usize::try_from(offsets[row]).map_err(|_| "negative list offset")?;
            let end = usize::try_from(offsets[row + 1]).map_err(|_| "negative list offset")?;
            container_sequence_upper_bound(
                array.values(),
                start,
                end,
                schema.and_then(FieldRenderSchema::list_item),
                field,
            )
        }
        DataType::LargeList(field) => {
            let array = downcast_array::<LargeListArray>(column, "LargeListArray")?;
            let offsets = array.value_offsets();
            let start = usize::try_from(offsets[row]).map_err(|_| "negative list offset")?;
            let end = usize::try_from(offsets[row + 1]).map_err(|_| "negative list offset")?;
            container_sequence_upper_bound(
                array.values(),
                start,
                end,
                schema.and_then(FieldRenderSchema::list_item),
                field,
            )
        }
        DataType::Map(entries, _) => {
            let array = downcast_array::<MapArray>(column, "MapArray")?;
            let fields = match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => fields,
                _ => return Err("invalid map entries type in MySQL result".to_string()),
            };
            let offsets = array.offsets();
            let start = usize::try_from(offsets[row]).map_err(|_| "negative map offset")?;
            let end = usize::try_from(offsets[row + 1]).map_err(|_| "negative map offset")?;
            let key_schema = schema
                .and_then(FieldRenderSchema::map_key)
                .cloned()
                .unwrap_or_else(|| FieldRenderSchema::from_field(fields[0].as_ref()));
            let value_schema = schema
                .and_then(FieldRenderSchema::map_value)
                .cloned()
                .unwrap_or_else(|| FieldRenderSchema::from_field(fields[1].as_ref()));
            let mut bytes = 2_u64;
            for index in start..end {
                if index > start {
                    bytes = checked_wire_add(bytes, 1)?;
                }
                bytes = checked_wire_add(
                    bytes,
                    mysql_container_upper_bound(array.keys(), index, Some(&key_schema), false)?,
                )?;
                bytes = checked_wire_add(bytes, 1)?;
                bytes = checked_wire_add(
                    bytes,
                    mysql_container_upper_bound(
                        array.values(),
                        index,
                        Some(&value_schema),
                        value_schema.is_json_value(),
                    )?,
                )?;
            }
            Ok(bytes)
        }
        DataType::Struct(fields) => {
            let array = downcast_array::<StructArray>(column, "StructArray")?;
            let mut bytes = 2_u64;
            for (index, field) in fields.iter().enumerate() {
                if index > 0 {
                    bytes = checked_wire_add(bytes, 1)?;
                }
                bytes = checked_wire_add(
                    bytes,
                    quoted_string_upper_bound(usize_to_u64(field.name().len())?, 2)?,
                )?;
                bytes = checked_wire_add(bytes, 1)?;
                let child_schema = schema
                    .and_then(|schema| schema.struct_child(index))
                    .cloned()
                    .unwrap_or_else(|| FieldRenderSchema::from_field(field.as_ref()));
                bytes = checked_wire_add(
                    bytes,
                    mysql_container_upper_bound(
                        array.column(index),
                        row,
                        Some(&child_schema),
                        child_schema.is_json_value(),
                    )?,
                )?;
            }
            Ok(bytes)
        }
        other => Err(format!(
            "unsupported array type in MySQL container result: {other:?}"
        )),
    }
}

fn container_sequence_upper_bound(
    values: &ArrayRef,
    start: usize,
    end: usize,
    schema: Option<&FieldRenderSchema>,
    field: &arrow::datatypes::Field,
) -> Result<u64, String> {
    let item_schema = schema
        .cloned()
        .unwrap_or_else(|| FieldRenderSchema::from_field(field));
    let mut bytes = 2_u64;
    for index in start..end {
        if index > start {
            bytes = checked_wire_add(bytes, 1)?;
        }
        bytes = checked_wire_add(
            bytes,
            mysql_container_upper_bound(
                values,
                index,
                Some(&item_schema),
                item_schema.is_json_value(),
            )?,
        )?;
    }
    Ok(bytes)
}

fn is_opaque_aggregate_column(name: &str) -> bool {
    let name = name.to_lowercase();
    name.starts_with("bitmap_agg(")
        || name.starts_with("bitmap_union(")
        || name.starts_with("hll_union(")
        || name.starts_with("hll_raw_agg(")
}

fn mysql_lenenc_string_upper_bound(bytes: u64) -> Result<u64, String> {
    let prefix = if bytes < 251 {
        1
    } else if bytes < 65_536 {
        3
    } else if bytes < 16_777_216 {
        4
    } else {
        9
    };
    checked_wire_add(bytes, prefix)
}

fn quoted_string_upper_bound(bytes: u64, expansion: u64) -> Result<u64, String> {
    bytes
        .checked_mul(expansion)
        .and_then(|value| value.checked_add(2))
        .ok_or_else(|| "MySQL quoted string upper bound overflow".to_string())
}

fn checked_wire_add(left: u64, right: u64) -> Result<u64, String> {
    left.checked_add(right)
        .ok_or_else(|| "MySQL protocol byte upper bound overflow".to_string())
}

fn usize_to_u64(value: usize) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| "MySQL value length does not fit u64".to_string())
}

fn downcast_array<'a, T: 'static>(column: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("failed to downcast output column to {expected}"))
}

fn invalid_query_result_delivery(message: impl Into<String>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, message.into())
}

fn failed_query_result_delivery(message: impl Into<String>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, message.into())
}

fn is_terminal_cancellation(error: &QueryExecutionError) -> bool {
    matches!(
        error.kind(),
        QueryExecutionErrorKind::Cancelled | QueryExecutionErrorKind::DeadlineExceeded
    )
}

fn normalize_terminal_cancellation(error: QueryExecutionError) -> QueryExecutionError {
    if error.kind() == QueryExecutionErrorKind::DeadlineExceeded {
        QueryExecutionError::new(QueryExecutionErrorKind::DeadlineExceeded, "query timed out")
    } else {
        error
    }
}

fn cancelled_query_result_delivery(reason: QueryCancellationReason) -> QueryExecutionError {
    let message = match reason {
        QueryCancellationReason::DeadlineExceeded { timeout_ms } => {
            format!("query timed out after {timeout_ms} ms")
        }
        QueryCancellationReason::ExecutionCancellationRequested => {
            "Query execution cancellation was requested".to_string()
        }
        QueryCancellationReason::ExecutionOwnerDropped => {
            "Query execution protocol owner was dropped".to_string()
        }
        QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms } => format!(
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded after {timeout_ms} ms"
        ),
        QueryCancellationReason::ExplicitKill { .. } => {
            "Query execution was interrupted".to_string()
        }
        QueryCancellationReason::ExplicitKillConnection { .. } => {
            "Query execution was interrupted because the connection was killed".to_string()
        }
        QueryCancellationReason::ClientDisconnected => {
            "Query execution was interrupted because the client disconnected".to_string()
        }
        QueryCancellationReason::ServerShutdown => {
            "Query execution was interrupted because the server is shutting down".to_string()
        }
    };
    QueryExecutionError::new(QueryExecutionErrorKind::Cancelled, message)
}

fn governed_cancelled_query_result_delivery(
    reason: novarocks_workload_control::CancellationReason,
) -> QueryExecutionError {
    let message = match reason {
        novarocks_workload_control::CancellationReason::FrontendDrainDeadlineExceeded => {
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded".to_string()
        }
        reason => format!("MySQL result delivery cancelled: {reason:?}"),
    };
    QueryExecutionError::new(QueryExecutionErrorKind::Cancelled, message)
}

fn interrupted_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, message.into())
}

fn invalid_data_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod streaming_result_tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_query_application::api::{
        QueryExecutionError, QueryExecutionErrorKind, ResultDelivery, ResultField,
    };
    use novarocks_query_application::test_support::{
        ResultStreamTestProducer, TestResultDeliveryDisposition,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use opensrv_mysql::ToMysqlValue;

    use super::*;
    use novarocks_query_application::client_connection::ClientConnectionToken;
    use novarocks_query_application::protocol_delivery::StreamingStatementResult;
    use novarocks_query_application::query_control::QueryApplicationControl;
    use novarocks_query_application::session_control::{
        QueryCancelOutcome, QueryControlPort, QueryControlService, QuerySessionLease,
        SessionIdentity,
    };

    struct Fixture {
        producer: ResultStreamTestProducer,
        result: Option<StreamingStatementResult>,
        resources: novarocks_workload_control::LocalResourceAuthority,
        _governance: WorkloadControl,
        control: QueryControlService,
        session: QuerySessionLease,
        schema_receipt: novarocks_query_application::test_support::TestResultDeliveryReceipt,
    }

    fn fixture(fields: Vec<ResultField>) -> Fixture {
        let execution_id = QueryExecutionId::new(
            QueryId::new(71, 1),
            AttemptId::new(1).expect("test attempt"),
        )
        .expect("test execution");
        let (producer, execution, resources, schema_receipt) = ResultStreamTestProducer::open(
            execution_id,
            fields,
            1,
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("open result stream");

        let port: Arc<dyn QueryControlPort> = Arc::new(QueryApplicationControl::default());
        let control = QueryControlService::new(port);
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(91, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let governance = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("test statement workload");
        governance.mark_ready().expect("statement workload ready");
        let mut statement = control
            .begin_governed_query_statement(
                session.token(),
                &governance.root_admission(),
                None,
                None,
            )
            .expect("begin governed statement");
        statement
            .take_execution_owner()
            .expect("test transfers execution owner")
            .complete();
        let result =
            StreamingStatementResult::try_from_execution(execution, resources.clone(), statement)
                .expect("bind streaming statement");
        Fixture {
            producer,
            result: Some(result),
            resources,
            _governance: governance,
            control,
            session,
            schema_receipt,
        }
    }

    fn string_fields() -> Vec<ResultField> {
        vec![ResultField::new("value", DataType::Utf8, false, None)]
    }

    fn string_batch(value: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![value]))],
        )
        .expect("string batch")
    }

    #[test]
    fn mysql_text_protocol_bound_measures_encoded_rows_not_decoded_width() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![i64::MIN]))],
        )
        .unwrap();
        let columns = vec![QueryResultColumn::new(
            "value",
            DataType::Int64,
            false,
            None,
        )];

        // i64 text has at most 20 digits, one length byte, and one four-byte
        // packet header. opensrv may retain one fallback copy after a partial
        // vectored write; the decoded fixed width is only eight.
        assert_eq!(
            mysql_text_batch_protocol_bytes_upper_bound(&batch, &columns).unwrap(),
            50
        );
    }

    #[test]
    fn schema_and_terminal_bounds_cover_packetwriter_fallback_buffers() {
        let name = "result_column";
        let bound = mysql_schema_protocol_bytes_upper_bound([name].into_iter(), 1).unwrap();
        let name_bytes = u64::try_from(name.len()).unwrap();
        let column_packet = mysql_lenenc_string_upper_bound(name_bytes).unwrap() + 25;
        assert!(
            bound >= name_bytes * 2 + column_packet * 2,
            "schema bound must cover carrier names and PacketWriter fallback"
        );
        // The largest empty-info terminal form is a 12-byte framed OK packet;
        // PacketWriter can retain one remaining-payload copy.
        assert!(MYSQL_TERMINAL_PROTOCOL_BYTES_UPPER_BOUND >= 24);
    }

    #[tokio::test]
    async fn terminal_protocol_reservation_uses_control_capacity_when_data_is_full() {
        let governance = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("test workload");
        governance.mark_ready().expect("test workload ready");
        let root = governance
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("test root work");
        let resources = governance.resources();
        let scope = root.owner.scope().clone();
        let data_limit = resources.snapshot().total_limit_bytes - 1024;
        let data = resources
            .reserve(&scope, data_limit, ResourceClass::Data)
            .expect("fill the data partition");

        let terminal = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reserve_terminal_protocol_when_available(resources.clone(), scope.clone()),
        )
        .await
        .expect("terminal control bytes must not wait behind data")
        .expect("reserve terminal control bytes");
        assert_eq!(
            resources.snapshot().control_reserved_bytes,
            MYSQL_TERMINAL_PROTOCOL_BYTES_UPPER_BOUND
        );
        drop(terminal);
        drop(data);
        drop(scope);
        root.business.release();
        root.owner.complete();
    }

    #[test]
    fn interrupted_writer_error_is_client_disconnect_not_cancellation() {
        let interrupted = ProtocolWriteFailure::Io(io::Error::new(
            io::ErrorKind::Interrupted,
            "socket write interrupted",
        ));
        assert_eq!(
            interrupted.settlement(),
            ProtocolWriteSettlement::ClientDisconnected
        );
        assert_eq!(
            ProtocolWriteFailure::Cancelled(cancelled_query_result_delivery(
                QueryCancellationReason::ClientDisconnected,
            ))
            .settlement(),
            ProtocolWriteSettlement::Cancellation
        );
    }

    #[test]
    fn deadline_delivery_cancellation_keeps_the_statement_timeout_message() {
        let error = cancelled_query_result_delivery(QueryCancellationReason::DeadlineExceeded {
            timeout_ms: 1_000,
        });
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert_eq!(error.to_string(), "query timed out after 1000 ms");
    }

    #[test]
    fn actor_deadline_terminal_is_settled_as_a_mysql_timeout() {
        let error = normalize_terminal_cancellation(QueryExecutionError::new(
            QueryExecutionErrorKind::DeadlineExceeded,
            "logical execution deadline expired before success EOF",
        ));

        assert!(is_terminal_cancellation(&error));
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);
        assert_eq!(error.to_string(), "query timed out");
    }

    #[test]
    fn explicit_kill_delivery_cancellation_keeps_the_statement_message() {
        let error = cancelled_query_result_delivery(QueryCancellationReason::ExplicitKill {
            requester_connection_id: 7,
        });
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert_eq!(error.to_string(), "Query execution was interrupted");
    }

    #[test]
    fn container_protocol_bound_covers_single_render_without_prerendering() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(-2),
        ])]);
        let field = Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field.clone()])),
            vec![Arc::new(list)],
        )
        .unwrap();
        let columns = vec![QueryResultColumn::new(
            "items",
            field.data_type().clone(),
            false,
            None,
        )];
        let upper = mysql_text_batch_protocol_bytes_upper_bound(&batch, &columns).unwrap();
        let values = crate::build_mysql_row(&batch, &columns, 0).unwrap();
        let mut rendered = Vec::new();
        for value in values {
            value.to_mysql_text(&mut rendered).unwrap();
        }
        let exact_wire = (u64::try_from(rendered.len()).unwrap() + 4) * 2;
        assert!(upper >= exact_wire, "upper={upper}, exact={exact_wire}");
    }

    #[tokio::test]
    async fn schema_batch_and_eof_complete_only_in_protocol_order() {
        let mut fixture = fixture(string_fields());
        let result = fixture.result.as_mut().unwrap();
        result.begin_schema().expect("schema").complete();
        assert_eq!(
            fixture.schema_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );

        let batch_receipt = fixture
            .producer
            .enqueue_batch(0, string_batch("abc"))
            .await
            .unwrap();
        let ResultDelivery::Batch(delivery) = result.next_delivery().await.unwrap().unwrap() else {
            panic!("expected batch")
        };
        let columns = result_schema_to_query_result_columns(&ResultSchema::new(string_fields()));
        let bound =
            mysql_text_batch_protocol_bytes_upper_bound(delivery.batch(), &columns).unwrap();
        assert_eq!(
            bound, 16,
            "wire row plus PacketWriter partial-write fallback"
        );
        let delivery = delivery
            .reserve_protocol_when_available(&fixture.resources, bound)
            .await
            .unwrap()
            .begin_protocol_write(bound)
            .unwrap();
        assert!(fixture.resources.snapshot().result_credit.held_bytes() > 0);
        delivery.complete().unwrap();
        assert_eq!(
            batch_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );
        assert_eq!(fixture.resources.snapshot().result_credit.held_bytes(), 0);

        let end_receipt = fixture.producer.enqueue_end(1).await;
        let ResultDelivery::End(end) = result.next_delivery().await.unwrap().unwrap() else {
            panic!("expected EOF")
        };
        end.complete();
        assert_eq!(
            end_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );
        fixture.result.take().unwrap().complete();
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::NoActiveStatement
        ));
        fixture.producer.finish();
    }

    #[tokio::test]
    async fn slow_protocol_owner_retains_batch_credit_until_actual_completion() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_batch(0, string_batch("slow"))
            .await
            .unwrap();
        let ResultDelivery::Batch(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        let columns = result_schema_to_query_result_columns(&ResultSchema::new(string_fields()));
        let bound =
            mysql_text_batch_protocol_bytes_upper_bound(delivery.batch(), &columns).unwrap();
        let delivery = delivery
            .reserve_protocol_when_available(&fixture.resources, bound)
            .await
            .unwrap()
            .begin_protocol_write(bound)
            .unwrap();

        tokio::task::yield_now().await;
        assert!(
            fixture
                .resources
                .snapshot()
                .result_credit
                .protocol_writing_bytes
                > 0
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), receipt.wait())
                .await
                .is_err()
        );
        delivery.complete().unwrap();
        assert_eq!(fixture.resources.snapshot().result_credit.held_bytes(), 0);
        drop(fixture.result.take());
        fixture.producer.finish();
    }

    #[tokio::test]
    async fn partial_protocol_failure_fails_receipt_and_releases_credit() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_batch(0, string_batch("partial"))
            .await
            .unwrap();
        let ResultDelivery::Batch(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        let delivery = delivery
            .reserve_protocol_when_available(&fixture.resources, 64)
            .await
            .unwrap()
            .begin_protocol_write(32)
            .unwrap();
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "client disconnected after a partial row write",
        ));
        assert!(matches!(
            receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        assert_eq!(fixture.resources.snapshot().result_credit.held_bytes(), 0);
        fixture.result.take().unwrap().fail();
        fixture.producer.finish();
    }

    #[tokio::test]
    async fn malformed_batch_is_explicitly_failed_before_protocol_write() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let malformed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new("unexpected", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["one"])),
                Arc::new(StringArray::from(vec!["two"])),
            ],
        )
        .unwrap();
        let receipt = fixture.producer.enqueue_batch(0, malformed).await.unwrap();
        let ResultDelivery::Batch(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected malformed batch")
        };
        let columns = result_schema_to_query_result_columns(&ResultSchema::new(string_fields()));
        let message = mysql_text_batch_protocol_bytes_upper_bound(delivery.batch(), &columns)
            .expect_err("column mismatch must fail closed");
        let error = invalid_query_result_delivery(message);
        delivery.fail(error);
        fixture.result.take().unwrap().fail();

        assert!(matches!(
            receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        assert_eq!(fixture.resources.snapshot().result_credit.held_bytes(), 0);
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        fixture.producer.finish();
    }

    #[tokio::test]
    async fn invalid_complete_transition_fails_receipt_and_statement_owner() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_batch(0, string_batch("invalid transition"))
            .await
            .unwrap();
        let ResultDelivery::Batch(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        delivery
            .complete()
            .expect_err("decoded delivery cannot skip protocol reservation and write");
        fixture.result.take().unwrap().fail();

        assert!(matches!(
            receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        fixture.producer.finish();
    }

    #[tokio::test]
    async fn native_failure_view_interrupts_an_in_progress_protocol_owner() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_batch(0, string_batch("blocked client"))
            .await
            .unwrap();
        let ResultDelivery::Batch(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        let mut failure = fixture.result.as_ref().unwrap().failure_view().unwrap();
        let expected = QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "native attempt failed during client backpressure",
        );
        fixture.producer.fail(expected.clone());
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_millis(50), failure.wait())
                .await
                .expect("failure observation must not wait for another batch"),
            expected
        );
        delivery.fail(expected);
        fixture.result.take().unwrap().fail();
        assert!(matches!(
            receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        fixture.producer.finish();
    }

    #[tokio::test]
    async fn owner_drop_cancels_execution_and_drops_undelivered_schema() {
        let mut fixture = fixture(string_fields());
        drop(fixture.result.take());
        assert_eq!(
            fixture.schema_receipt.wait().await,
            TestResultDeliveryDisposition::Dropped
        );
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::NoActiveStatement
        ));
        fixture.producer.finish();
    }

    #[tokio::test]
    async fn query_control_cancellation_remains_live_until_stream_owner_finishes() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::Requested
        ));
        assert_eq!(
            fixture.result.as_ref().unwrap().cancellation().reason(),
            Some(QueryCancellationReason::ClientDisconnected)
        );
        fixture.result.take().unwrap().fail();
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        fixture.producer.finish();
    }
}
