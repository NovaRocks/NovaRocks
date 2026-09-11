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

//! Arrow → MySQL wire value conversion for the standalone MySQL server.

use std::io::{self, Write};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use mysql_common::value::Value as MySqlValue;
use opensrv_mysql::{
    Column, ColumnFlags, ColumnType, ErrorKind, QueryResultWriter, ToMysqlValue, U24_MAX,
};
use tokio::io::AsyncWrite;

use crate::query_execution::control::GovernedStatementVisibilitySealOutcome;
use crate::runtime::query_result::{QueryResult, QueryResultColumn};
use crate::runtime::statement_result::{
    GovernedImmediateStatementResult, StreamingStatementResult,
};
use novarocks_execution::exec::chunk::Chunk;
use novarocks_query_application::api::{
    QueryExecutionError, QueryExecutionErrorKind, ResultDelivery, ResultFailureView, ResultSchema,
};
use novarocks_types::{FieldRenderSchema, format_mysql_container_value_with_schema};
use novarocks_workload_control::{
    LocalResourceAuthority, Reservation, ResourceClass, WorkError, WorkScope,
};

const MYSQL_TERMINAL_PROTOCOL_BYTES_UPPER_BOUND: u64 = 64;

enum ProtocolFinishFailure {
    Native(QueryExecutionError),
    Io(io::Error),
}

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

impl ProtocolWriteFailure {
    fn settlement(&self) -> ProtocolWriteSettlement {
        match self {
            Self::Cancelled(_) => ProtocolWriteSettlement::Cancellation,
            Self::Native(_) | Self::Encoding(_) => ProtocolWriteSettlement::ProtocolFailed,
            Self::Io(_) => ProtocolWriteSettlement::ClientDisconnected,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum StandaloneMysqlValue {
    Null,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time {
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
}

impl ToMysqlValue for StandaloneMysqlValue {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            Self::Null => None::<u8>.to_mysql_text(w),
            Self::Bytes(bytes) => bytes.to_mysql_text(w),
            Self::Int(value) => value.to_mysql_text(w),
            Self::UInt(value) => value.to_mysql_text(w),
            Self::Float(value) => value.to_mysql_text(w),
            Self::Double(value) => value.to_mysql_text(w),
            Self::Date(value) => value.to_mysql_text(w),
            Self::DateTime(value) => value.to_mysql_text(w),
            Self::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            } => MySqlValue::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
                .to_mysql_text(w),
        }
    }

    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match self {
            Self::Null => unreachable!("NULL payloads are handled by the row null bitmap"),
            Self::Bytes(bytes) => bytes.to_mysql_bin(w, c),
            Self::Int(value) => value.to_mysql_bin(w, c),
            Self::UInt(value) => value.to_mysql_bin(w, c),
            Self::Float(value) => value.to_mysql_bin(w, c),
            Self::Double(value) => value.to_mysql_bin(w, c),
            Self::Date(value) => value.to_mysql_bin(w, c),
            Self::DateTime(value) => value.to_mysql_bin(w, c),
            Self::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            } => MySqlValue::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
                .to_mysql_bin(w, c),
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

pub(super) async fn write_query_result<W: AsyncWrite + Unpin>(
    result: QueryResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let columns = result
        .columns
        .iter()
        .map(query_result_column_to_mysql_column)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid_data_error)?;

    let mut writer = results.start(columns.as_slice()).await?;
    for chunk in &result.chunks {
        for row_idx in 0..chunk.len() {
            let row =
                build_mysql_row(chunk, &result.columns, row_idx).map_err(invalid_data_error)?;
            writer.write_row(row).await?;
        }
    }
    writer.finish().await
}

pub(super) async fn write_governed_query_result<W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let (result, mut protocol) = result.into_parts();
    let schema_bytes = match mysql_query_result_schema_protocol_bytes_upper_bound(&result.columns) {
        Ok(bytes) => bytes,
        Err(message) => {
            let error = invalid_query_result_delivery(message);
            let _ = protocol.fail();
            let message = error.to_string().into_bytes();
            return results.error(ErrorKind::ER_UNKNOWN_ERROR, &message).await;
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
            return results.error(ErrorKind::ER_UNKNOWN_ERROR, &message).await;
        }
    };
    let mysql_columns = match result
        .columns
        .iter()
        .map(query_result_column_to_mysql_column)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(columns) => columns,
        Err(message) => {
            let error = invalid_query_result_delivery(message);
            let _ = protocol.fail();
            let message = error.to_string().into_bytes();
            return results.error(ErrorKind::ER_UNKNOWN_ERROR, &message).await;
        }
    };
    let cancellation = protocol.cancellation();
    let start = results.start(mysql_columns.as_slice());
    tokio::pin!(start);
    let mut writer = tokio::select! {
        biased;
        reason = cancellation.cancelled() => {
            let error = cancelled_query_result_delivery(reason);
            let _ = protocol.settle_cancellation();
            return Err(interrupted_error(error.to_string()));
        }
        opened = &mut start => match opened {
            Ok(writer) => writer,
            Err(error) => {
                let _ = protocol.client_disconnected();
                return Err(error);
            }
        }
    };
    drop(schema_reservation);

    for chunk in &result.chunks {
        let protocol_bytes =
            match mysql_text_batch_protocol_bytes_upper_bound(chunk, &result.columns) {
                Ok(bytes) => bytes,
                Err(message) => {
                    let error = invalid_query_result_delivery(message);
                    let _ = protocol.fail();
                    return finish_stream_error(writer, ErrorKind::ER_UNKNOWN_ERROR, &error).await;
                }
            };
        let cancellation = protocol.cancellation();
        let (resources, scope) = protocol.reservation_inputs();
        let reserve = reserve_data_when_available(resources, scope, protocol_bytes);
        tokio::pin!(reserve);
        let row_reservation = match tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                Err(cancelled_query_result_delivery(reason))
            }
            reservation = &mut reserve => reservation.map_err(|error| {
                failed_query_result_delivery(format!("reserve MySQL result bytes: {error}"))
            })
        } {
            Ok(reservation) => reservation,
            Err(error) => {
                if error.kind() == QueryExecutionErrorKind::Cancelled {
                    let _ = protocol.settle_cancellation();
                } else {
                    let _ = protocol.fail();
                }
                return finish_stream_error(writer, ErrorKind::ER_UNKNOWN_ERROR, &error).await;
            }
        };
        if let Err(error) =
            write_governed_batch(&mut writer, chunk, &result.columns, protocol.cancellation()).await
        {
            let settlement = error.settlement();
            match error {
                ProtocolWriteFailure::Cancelled(error) => {
                    debug_assert_eq!(settlement, ProtocolWriteSettlement::Cancellation);
                    let _ = protocol.settle_cancellation();
                    return Err(interrupted_error(error.to_string()));
                }
                ProtocolWriteFailure::Encoding(error) => {
                    debug_assert_eq!(settlement, ProtocolWriteSettlement::ProtocolFailed);
                    let _ = protocol.fail();
                    return Err(error);
                }
                ProtocolWriteFailure::Io(error) => {
                    debug_assert_eq!(settlement, ProtocolWriteSettlement::ClientDisconnected);
                    // An I/O error, including `Interrupted`, is evidence about
                    // the socket only. Cancellation is settled exclusively by
                    // the cancellation branch above.
                    let _ = protocol.client_disconnected();
                    return Err(error);
                }
                ProtocolWriteFailure::Native(_) => {
                    unreachable!("immediate results have no native failure view")
                }
            }
        }
        drop(row_reservation);
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
            return finish_stream_error(writer, ErrorKind::ER_UNKNOWN_ERROR, &error).await;
        }
    };
    match protocol.seal_success_visibility() {
        GovernedStatementVisibilitySealOutcome::Sealed => {}
        GovernedStatementVisibilitySealOutcome::Cancelled(reason) => {
            let error = governed_cancelled_query_result_delivery(reason);
            let _ = protocol.settle_cancellation();
            return finish_stream_error(writer, ErrorKind::ER_QUERY_INTERRUPTED, &error).await;
        }
        GovernedStatementVisibilitySealOutcome::Stale => {
            let error = failed_query_result_delivery(
                "governed query lost its statement generation before success visibility",
            );
            let _ = protocol.fail();
            return finish_stream_error(writer, ErrorKind::ER_UNKNOWN_ERROR, &error).await;
        }
    }
    match writer.finish().await {
        Ok(()) => {
            drop(terminal_reservation);
            let _ = protocol.complete();
            Ok(())
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
pub(super) async fn write_streaming_query_result<W: AsyncWrite + Unpin>(
    mut result: StreamingStatementResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
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
                return Err(invalid_data_error(error.to_string()));
            }
            reason = cancellation.cancelled() => {
                let error = cancelled_query_result_delivery(reason);
                schema_delivery.fail(error.clone());
                let _ = result.settle_cancellation();
                return Err(interrupted_error(error.to_string()));
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
    let mysql_columns = match columns
        .iter()
        .map(query_result_column_to_mysql_column)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(columns) => columns,
        Err(error) => {
            let error = invalid_query_result_delivery(error);
            schema_delivery.fail(error.clone());
            let _ = result.fail();
            return Err(invalid_data_error(error.to_string()));
        }
    };

    let cancellation = result.cancellation();
    let start = results.start(mysql_columns.as_slice());
    tokio::pin!(start);
    let mut writer = tokio::select! {
        biased;
        error = failure.wait() => {
            schema_delivery.fail(error.clone());
            let _ = result.fail();
            return Err(invalid_data_error(error.to_string()));
        }
        reason = cancellation.cancelled() => {
            let error = cancelled_query_result_delivery(reason);
            schema_delivery.fail(error.clone());
            let _ = result.settle_cancellation();
            return Err(interrupted_error(error.to_string()));
        }
        opened = &mut start => match opened {
            Ok(writer) => writer,
            Err(error) => {
                schema_delivery.fail(failed_query_result_delivery(
                    format!("write MySQL result schema: {error}"),
                ));
                let _ = result.client_disconnected();
                return Err(error);
            }
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
                let kind = if error.kind() == QueryExecutionErrorKind::Cancelled {
                    ErrorKind::ER_QUERY_INTERRUPTED
                } else {
                    ErrorKind::ER_UNKNOWN_ERROR
                };
                if error.kind() == QueryExecutionErrorKind::Cancelled {
                    let _ = result.settle_cancellation();
                } else {
                    let _ = result.fail();
                }
                return finish_stream_error(writer, kind, &error).await;
            }
        };

        match delivery {
            ResultDelivery::Batch(delivery) => {
                let chunk = match crate::runtime::query_result::record_batch_to_chunk(
                    delivery.batch().clone(),
                ) {
                    Ok(chunk) => chunk,
                    Err(message) => {
                        let error = invalid_query_result_delivery(format!(
                            "decode MySQL result batch metadata: {message}"
                        ));
                        delivery.fail(error.clone());
                        let _ = result.fail();
                        return finish_stream_error(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                            .await;
                    }
                };
                let protocol_bytes =
                    match mysql_text_batch_protocol_bytes_upper_bound(&chunk, &columns) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            let error = invalid_query_result_delivery(error);
                            delivery.fail(error.clone());
                            let _ = result.fail();
                            return finish_stream_error(
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
                        let kind = if error.kind() == QueryExecutionErrorKind::Cancelled {
                            ErrorKind::ER_QUERY_INTERRUPTED
                        } else {
                            ErrorKind::ER_UNKNOWN_ERROR
                        };
                        if error.kind() == QueryExecutionErrorKind::Cancelled {
                            let _ = result.settle_cancellation();
                        } else {
                            let _ = result.fail();
                        }
                        return finish_stream_error(writer, kind, &error).await;
                    }
                };
                let delivery = match delivery.begin_protocol_write(protocol_bytes) {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        let _ = result.fail();
                        return finish_stream_error(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                            .await;
                    }
                };
                let cancellation = result.cancellation();
                if let Err(error) = write_streaming_batch(
                    &mut writer,
                    &chunk,
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
                    return finish_stream_error(writer, ErrorKind::ER_UNKNOWN_ERROR, &error).await;
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
                        let kind = if error.kind() == QueryExecutionErrorKind::Cancelled {
                            ErrorKind::ER_QUERY_INTERRUPTED
                        } else {
                            ErrorKind::ER_UNKNOWN_ERROR
                        };
                        delivery.fail(error.clone());
                        if error.kind() == QueryExecutionErrorKind::Cancelled {
                            let _ = result.settle_cancellation();
                        } else {
                            let _ = result.fail();
                        }
                        return finish_stream_error(writer, kind, &error).await;
                    }
                };
                match result.seal_success_visibility() {
                    GovernedStatementVisibilitySealOutcome::Sealed => {}
                    GovernedStatementVisibilitySealOutcome::Cancelled(reason) => {
                        let error = governed_cancelled_query_result_delivery(reason);
                        delivery.fail(error.clone());
                        let _ = result.settle_cancellation();
                        return finish_stream_error(
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
                        return finish_stream_error(writer, ErrorKind::ER_UNKNOWN_ERROR, &error)
                            .await;
                    }
                }
                let finish = writer.finish();
                tokio::pin!(finish);
                let finished = tokio::select! {
                    biased;
                    error = failure.wait() => Err(ProtocolFinishFailure::Native(error)),
                    finished = &mut finish => finished.map_err(ProtocolFinishFailure::Io),
                };
                match finished {
                    Ok(()) => {
                        drop(terminal_reservation);
                        delivery.complete();
                        let _ = result.complete();
                        return Ok(());
                    }
                    Err(ProtocolFinishFailure::Native(error)) => {
                        drop(terminal_reservation);
                        delivery.fail(error.clone());
                        let _ = result.fail();
                        return Err(invalid_data_error(error.to_string()));
                    }
                    Err(ProtocolFinishFailure::Io(error)) => {
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
    writer: opensrv_mysql::RowWriter<'_, W>,
    kind: ErrorKind,
    error: &QueryExecutionError,
) -> io::Result<()> {
    let message = error.to_string().into_bytes();
    writer.finish_error(kind, &message).await
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
    writer: &mut opensrv_mysql::RowWriter<'_, W>,
    chunk: &Chunk,
    columns: &[QueryResultColumn],
    cancellation: crate::common::query_cancellation::QueryCancellationView,
    mut failure: ResultFailureView,
) -> Result<(), ProtocolWriteFailure> {
    for row_idx in 0..chunk.len() {
        let values = build_mysql_row(chunk, columns, row_idx)
            .map_err(invalid_data_error)
            .map_err(ProtocolWriteFailure::Encoding)?;
        let write = writer.write_row(values);
        tokio::pin!(write);
        tokio::select! {
            biased;
            error = failure.wait() => {
                return Err(ProtocolWriteFailure::Native(error));
            }
            reason = cancellation.cancelled() => {
                return Err(ProtocolWriteFailure::Cancelled(
                    cancelled_query_result_delivery(reason)
                ));
            }
            written = &mut write => written.map_err(ProtocolWriteFailure::Io)?,
        }
    }
    Ok(())
}

async fn write_governed_batch<W: AsyncWrite + Unpin>(
    writer: &mut opensrv_mysql::RowWriter<'_, W>,
    chunk: &Chunk,
    columns: &[QueryResultColumn],
    cancellation: crate::common::query_cancellation::QueryCancellationView,
) -> Result<(), ProtocolWriteFailure> {
    for row_idx in 0..chunk.len() {
        let values = build_mysql_row(chunk, columns, row_idx)
            .map_err(invalid_data_error)
            .map_err(ProtocolWriteFailure::Encoding)?;
        let write = writer.write_row(values);
        tokio::pin!(write);
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                return Err(ProtocolWriteFailure::Cancelled(
                    cancelled_query_result_delivery(reason)
                ));
            }
            written = &mut write => written.map_err(ProtocolWriteFailure::Io)?,
        }
    }
    Ok(())
}

fn result_schema_to_query_result_columns(schema: &ResultSchema) -> Vec<QueryResultColumn> {
    schema
        .fields()
        .iter()
        .map(|field| QueryResultColumn {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.nullable(),
            logical_type: field.logical_type().cloned(),
        })
        .collect()
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
        columns.iter().map(|column| column.name.as_str()),
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
    chunk: &Chunk,
    columns: &[QueryResultColumn],
) -> Result<u64, String> {
    let mut largest = 0_u64;
    for row in 0..chunk.len() {
        if chunk.columns().len() != columns.len()
            || chunk.chunk_schema().slots().len() != columns.len()
        {
            return Err("query result columns do not match Arrow batch".to_string());
        }
        let mut payload = 0_u64;
        for ((column, slot), declared) in chunk
            .columns()
            .iter()
            .zip(chunk.chunk_schema().slots())
            .zip(columns)
        {
            let schema = FieldRenderSchema::from_field(slot.field());
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
        .logical_type
        .as_ref()
        .is_some_and(|logical| matches!(logical, novarocks_types::schema::SqlType::Decimal { .. }))
    {
        return mysql_lenenc_string_upper_bound(48);
    }
    if matches!(declared.data_type, DataType::Date32)
        && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return mysql_lenenc_string_upper_bound(10);
    }
    if matches!(
        declared.data_type,
        DataType::Time32(_) | DataType::Time64(_)
    ) && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return mysql_lenenc_string_upper_bound(32);
    }
    if schema.renders_opaque_binary()
        || (matches!(column.data_type(), DataType::Binary | DataType::LargeBinary)
            && is_opaque_aggregate_column(&declared.name))
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

fn invalid_query_result_delivery(message: impl Into<String>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, message.into())
}

fn failed_query_result_delivery(message: impl Into<String>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, message.into())
}

fn cancelled_query_result_delivery(
    reason: crate::common::query_cancellation::QueryCancellationReason,
) -> QueryExecutionError {
    QueryExecutionError::new(
        QueryExecutionErrorKind::Cancelled,
        format!("MySQL result delivery cancelled: {reason:?}"),
    )
}

fn governed_cancelled_query_result_delivery(
    reason: novarocks_workload_control::CancellationReason,
) -> QueryExecutionError {
    QueryExecutionError::new(
        QueryExecutionErrorKind::Cancelled,
        format!("MySQL result delivery cancelled: {reason:?}"),
    )
}

fn interrupted_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, message.into())
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

    use super::*;
    use crate::client_connection::ClientConnectionToken;
    use crate::common::query_cancellation::QueryCancellationReason;
    use crate::query_control::FrontendQueryControl;
    use crate::query_execution::control::{
        QueryControlPort, QueryControlService, QuerySessionLease, SessionIdentity,
    };
    use crate::runtime::statement_result::StreamingStatementResult;

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

        let port: Arc<dyn QueryControlPort> = Arc::new(FrontendQueryControl::default());
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
        let chunk = crate::runtime::query_result::record_batch_to_chunk(batch).unwrap();
        let columns = vec![QueryResultColumn {
            name: "value".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            logical_type: None,
        }];

        // i64 text has at most 20 digits, one length byte, and one four-byte
        // packet header. opensrv may retain one fallback copy after a partial
        // vectored write; the decoded fixed width is only eight.
        assert_eq!(
            mysql_text_batch_protocol_bytes_upper_bound(&chunk, &columns).unwrap(),
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
        let chunk = crate::runtime::query_result::record_batch_to_chunk(batch).unwrap();
        let columns = vec![QueryResultColumn {
            name: "items".to_string(),
            data_type: field.data_type().clone(),
            nullable: false,
            logical_type: None,
        }];
        let upper = mysql_text_batch_protocol_bytes_upper_bound(&chunk, &columns).unwrap();
        let values = build_mysql_row(&chunk, &columns, 0).unwrap();
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
        let chunk =
            crate::runtime::query_result::record_batch_to_chunk(delivery.batch().clone()).unwrap();
        let columns = result_schema_to_query_result_columns(&ResultSchema::new(string_fields()));
        let bound = mysql_text_batch_protocol_bytes_upper_bound(&chunk, &columns).unwrap();
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
            crate::query_execution::control::QueryCancelOutcome::NoActiveStatement
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
        let chunk =
            crate::runtime::query_result::record_batch_to_chunk(delivery.batch().clone()).unwrap();
        let columns = result_schema_to_query_result_columns(&ResultSchema::new(string_fields()));
        let bound = mysql_text_batch_protocol_bytes_upper_bound(&chunk, &columns).unwrap();
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
        let chunk =
            crate::runtime::query_result::record_batch_to_chunk(delivery.batch().clone()).unwrap();
        let columns = result_schema_to_query_result_columns(&ResultSchema::new(string_fields()));
        let message = mysql_text_batch_protocol_bytes_upper_bound(&chunk, &columns)
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
            crate::query_execution::control::QueryCancelOutcome::NoActiveStatement
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
            crate::query_execution::control::QueryCancelOutcome::Requested
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

pub(super) fn query_result_column_to_mysql_column(
    column: &QueryResultColumn,
) -> Result<Column, String> {
    let mut colflags = ColumnFlags::empty();
    if !column.nullable {
        colflags.insert(ColumnFlags::NOT_NULL_FLAG);
    }
    if matches!(
        column.logical_type,
        Some(novarocks_types::schema::SqlType::Decimal { .. })
    ) {
        return Ok(Column {
            table: String::new(),
            column: column.name.clone(),
            coltype: ColumnType::MYSQL_TYPE_NEWDECIMAL,
            colflags,
        });
    }
    let coltype = match column.data_type {
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            colflags.insert(ColumnFlags::UNSIGNED_FLAG);
            ColumnType::MYSQL_TYPE_LONGLONG
        }
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::FixedSizeBinary(width)
            if width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            ColumnType::MYSQL_TYPE_STRING
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::List(_)
        | DataType::Map(_, _)
        | DataType::Struct(_) => ColumnType::MYSQL_TYPE_VAR_STRING,
        DataType::Decimal128(_, _) => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        DataType::Date32 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Time32(_) | DataType::Time64(_) => ColumnType::MYSQL_TYPE_TIME,
        DataType::Timestamp(_, _) => ColumnType::MYSQL_TYPE_DATETIME,
        DataType::Null => ColumnType::MYSQL_TYPE_NULL,
        ref other => {
            return Err(format!(
                "standalone mysql server does not support output column type {:?}",
                other
            ));
        }
    };

    Ok(Column {
        table: String::new(),
        column: column.name.clone(),
        coltype,
        colflags,
    })
}

pub(super) fn build_mysql_row(
    chunk: &Chunk,
    columns: &[QueryResultColumn],
    row_idx: usize,
) -> Result<Vec<StandaloneMysqlValue>, String> {
    if chunk.columns().len() != columns.len() {
        return Err(format!(
            "query result column count mismatch: metadata has {}, chunk has {}",
            columns.len(),
            chunk.columns().len()
        ));
    }
    if chunk.chunk_schema().slots().len() != columns.len() {
        return Err(format!(
            "query result slot count mismatch: schema has {}, metadata has {}",
            chunk.chunk_schema().slots().len(),
            columns.len()
        ));
    }
    chunk
        .columns()
        .iter()
        .zip(chunk.chunk_schema().slots().iter())
        .zip(columns.iter())
        .map(|((column, slot), declared)| {
            let field_schema = FieldRenderSchema::from_field(slot.field());
            array_value_to_mysql_value(column, declared, row_idx, Some(&field_schema))
        })
        .collect()
}

pub(super) fn array_value_to_mysql_value(
    column: &ArrayRef,
    declared: &QueryResultColumn,
    row_idx: usize,
    field_schema: Option<&FieldRenderSchema>,
) -> Result<StandaloneMysqlValue, String> {
    if column.is_null(row_idx) {
        return Ok(StandaloneMysqlValue::Null);
    }

    if let Some(novarocks_types::schema::SqlType::Decimal { scale, .. }) =
        declared.logical_type.as_ref()
    {
        return decimal_to_mysql_value(column, row_idx, *scale);
    }

    if matches!(declared.data_type, DataType::Date32)
        && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return timestamp_to_date_mysql_value(column, timestamp_unit(column.data_type())?, row_idx);
    }
    if matches!(
        declared.data_type,
        DataType::Time32(_) | DataType::Time64(_)
    ) && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return timestamp_to_time_mysql_value(column, timestamp_unit(column.data_type())?, row_idx);
    }

    if field_schema.is_some_and(FieldRenderSchema::renders_opaque_binary) {
        return Ok(StandaloneMysqlValue::Null);
    }

    let name_lower = declared.name.to_lowercase();
    if matches!(column.data_type(), DataType::Binary | DataType::LargeBinary)
        && (name_lower.starts_with("bitmap_agg(")
            || name_lower.starts_with("bitmap_union(")
            || name_lower.starts_with("hll_union(")
            || name_lower.starts_with("hll_raw_agg("))
    {
        return Ok(StandaloneMysqlValue::Null);
    }

    match column.data_type() {
        DataType::Boolean => downcast_array::<BooleanArray>(column, "BooleanArray")
            .map(|arr| StandaloneMysqlValue::Int(if arr.value(row_idx) { 1 } else { 0 })),
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| StandaloneMysqlValue::Int(arr.value(row_idx))),
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| StandaloneMysqlValue::UInt(arr.value(row_idx))),
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| StandaloneMysqlValue::Float(arr.value(row_idx))),
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| StandaloneMysqlValue::Double(arr.value(row_idx))),
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let arr = downcast_array::<FixedSizeBinaryArray>(column, "FixedSizeBinaryArray")?;
            let value = novarocks_types::largeint::i128_from_be_bytes(arr.value(row_idx))?;
            Ok(StandaloneMysqlValue::Bytes(value.to_string().into_bytes()))
        }
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::Binary => downcast_array::<BinaryArray>(column, "BinaryArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::LargeBinary => downcast_array::<LargeBinaryArray>(column, "LargeBinaryArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(column, "Date32Array")?;
            date32_to_mysql_value(arr.value(row_idx))
        }
        DataType::Decimal128(_, scale) => decimal128_to_mysql_value(column, row_idx, *scale),
        DataType::Time32(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Time64(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Timestamp(unit, _) => timestamp_to_mysql_value(column, *unit, row_idx),
        DataType::Null => Ok(StandaloneMysqlValue::Null),
        DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            Ok(StandaloneMysqlValue::Bytes(
                format_mysql_container_value_with_schema(column, row_idx, field_schema)?
                    .into_bytes(),
            ))
        }
        other => Err(format!(
            "standalone mysql server does not support output column type {:?}",
            other
        )),
    }
}

fn decimal128_to_mysql_value(
    column: &ArrayRef,
    row_idx: usize,
    scale: i8,
) -> Result<StandaloneMysqlValue, String> {
    let arr = downcast_array::<Decimal128Array>(column, "Decimal128Array")?;
    Ok(StandaloneMysqlValue::Bytes(
        format_decimal128_string(arr.value(row_idx), scale)?.into_bytes(),
    ))
}

fn format_decimal128_string(value: i128, scale: i8) -> Result<String, String> {
    if scale < 0 {
        return Err(format!("unsupported decimal scale: {scale}"));
    }
    let scale = u32::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let factor = 10_u128
        .checked_pow(scale)
        .ok_or_else(|| format!("unsupported decimal scale: {scale}"))?;
    let negative = value.is_negative();
    let abs = value.unsigned_abs();
    let whole = abs / factor;
    let fraction = abs % factor;
    Ok(format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        whole,
        fraction,
        width = scale as usize
    ))
}

fn decimal_to_mysql_value(
    column: &ArrayRef,
    row_idx: usize,
    scale: i8,
) -> Result<StandaloneMysqlValue, String> {
    let scale =
        usize::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    let formatted = match column.data_type() {
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx) as f64))?,
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx) as f64))?,
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx)))?,
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| arr.value(row_idx).to_string())?,
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| arr.value(row_idx).to_string())?,
        other => {
            return Err(format!(
                "standalone mysql server does not support decimal output column type {:?}",
                other
            ));
        }
    };
    Ok(StandaloneMysqlValue::Bytes(formatted.into_bytes()))
}

fn timestamp_unit(data_type: &DataType) -> Result<TimeUnit, String> {
    match data_type {
        DataType::Timestamp(unit, _) => Ok(*unit),
        other => Err(format!("expected timestamp data type, got {:?}", other)),
    }
}

fn downcast_array<'a, T: 'static>(column: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("failed to downcast output column to {}", expected))
}

fn date32_to_mysql_value(days: i32) -> Result<StandaloneMysqlValue, String> {
    if days == novarocks_execution::exec::expr::function::date::zero_date_sentinel_date32() {
        return Ok(StandaloneMysqlValue::Bytes(b"0000-00-00".to_vec()));
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let date = epoch
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| format!("date32 value out of range: {days}"))?;
    Ok(StandaloneMysqlValue::Date(date))
}

fn timestamp_to_naive_datetime(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<NaiveDateTime, String> {
    let raw = timestamp_raw_micros(column, unit, row_idx)?;
    let secs = raw.div_euclid(1_000_000);
    let micros = raw.rem_euclid(1_000_000);
    let secs = i64::try_from(secs).map_err(|_| format!("timestamp value out of range: {raw}"))?;
    let micros =
        u32::try_from(micros).map_err(|_| format!("timestamp micros out of range: {raw}"))?;
    let dt = chrono::DateTime::<Utc>::from_timestamp(secs, micros * 1_000)
        .ok_or_else(|| format!("timestamp value out of range: {raw}"))?;
    Ok(dt.naive_utc())
}

fn timestamp_raw_micros(column: &ArrayRef, unit: TimeUnit, row_idx: usize) -> Result<i128, String> {
    let raw = match unit {
        TimeUnit::Second => {
            i128::from(
                downcast_array::<TimestampSecondArray>(column, "TimestampSecondArray")?
                    .value(row_idx),
            ) * 1_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(
                downcast_array::<TimestampMillisecondArray>(column, "TimestampMillisecondArray")?
                    .value(row_idx),
            ) * 1_000
        }
        TimeUnit::Microsecond => i128::from(
            downcast_array::<TimestampMicrosecondArray>(column, "TimestampMicrosecondArray")?
                .value(row_idx),
        ),
        TimeUnit::Nanosecond => {
            i128::from(
                downcast_array::<TimestampNanosecondArray>(column, "TimestampNanosecondArray")?
                    .value(row_idx),
            ) / 1_000
        }
    };
    Ok(raw)
}

fn timestamp_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    Ok(StandaloneMysqlValue::DateTime(timestamp_to_naive_datetime(
        column, unit, row_idx,
    )?))
}

fn timestamp_to_date_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    Ok(StandaloneMysqlValue::Date(
        timestamp_to_naive_datetime(column, unit, row_idx)?.date(),
    ))
}

fn timestamp_to_time_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    time_micros_to_mysql_value(timestamp_raw_micros(column, unit, row_idx)?)
}

fn time_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    let micros = match unit {
        TimeUnit::Second => {
            i128::from(
                downcast_array::<Time32SecondArray>(column, "Time32SecondArray")?.value(row_idx),
            ) * 1_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(
                downcast_array::<Time32MillisecondArray>(column, "Time32MillisecondArray")?
                    .value(row_idx),
            ) * 1_000
        }
        TimeUnit::Microsecond => i128::from(
            downcast_array::<Time64MicrosecondArray>(column, "Time64MicrosecondArray")?
                .value(row_idx),
        ),
        TimeUnit::Nanosecond => {
            i128::from(
                downcast_array::<Time64NanosecondArray>(column, "Time64NanosecondArray")?
                    .value(row_idx),
            ) / 1_000
        }
    };

    time_micros_to_mysql_value(micros)
}

fn time_micros_to_mysql_value(micros: i128) -> Result<StandaloneMysqlValue, String> {
    let total_seconds = micros.div_euclid(1_000_000);
    let microseconds = micros.rem_euclid(1_000_000) as u32;
    let hours = total_seconds.div_euclid(3_600);
    let minutes = total_seconds.rem_euclid(3_600).div_euclid(60);
    let seconds = total_seconds.rem_euclid(60);
    let days = hours.div_euclid(24);
    let hour_of_day = hours.rem_euclid(24);

    Ok(StandaloneMysqlValue::Time {
        negative: micros.is_negative(),
        days: u32::try_from(days.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        hours: u8::try_from(hour_of_day.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        minutes: u8::try_from(minutes.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        seconds: u8::try_from(seconds.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        micros: microseconds,
    })
}

pub(super) fn invalid_data_error(err: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BinaryArray, ListBuilder, StringBuilder, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use novarocks_types::SlotId;
    use novarocks_types::logical::{LogicalType, field_with_logical_type};

    #[test]
    fn declared_date_timestamp_value_serializes_without_time_component() {
        let declared = QueryResultColumn {
            name: "d".to_string(),
            data_type: DataType::Date32,
            nullable: false,
            logical_type: None,
        };
        let value = array_value_to_mysql_value(
            &(Arc::new(TimestampMicrosecondArray::from(vec![
                1_580_601_600_000_000i64,
            ])) as ArrayRef),
            &declared,
            0,
            None,
        )
        .expect("convert timestamp to DATE");

        assert_eq!(
            value,
            StandaloneMysqlValue::Date(NaiveDate::from_ymd_opt(2020, 2, 2).expect("valid date"))
        );

        let mut encoded = Vec::new();
        value
            .to_mysql_text(&mut encoded)
            .expect("encode DATE text payload");
        assert_eq!(encoded[0], 10);
        assert_eq!(&encoded[1..], b"2020-02-02");
    }

    #[test]
    fn build_mysql_row_uses_arrow_field_metadata_for_array_json() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value(r#"{"2:3": null}"#);
        builder.append(true);
        let raw_array = Arc::new(builder.finish()) as ArrayRef;
        let payload_field = Field::new(
            "payload",
            DataType::List(Arc::new(field_with_logical_type(
                Field::new("item", DataType::Utf8, true),
                LogicalType::Json,
            ))),
            true,
        );
        let array = novarocks_execution::exec::chunk::type_compatibility::retag_column(
            &raw_array,
            payload_field.data_type(),
        )
        .expect("retag array with logical metadata");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![payload_field.clone()])),
            vec![Arc::clone(&array)],
        )
        .expect("batch");
        let chunk = Chunk::new_with_chunk_schema(
            batch,
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    payload_field,
                    None,
                    None,
                )])
                .expect("chunk schema"),
            ),
        );
        let columns = vec![QueryResultColumn {
            name: "payload".to_string(),
            data_type: array.data_type().clone(),
            nullable: true,
            logical_type: None,
        }];

        let row = build_mysql_row(&chunk, &columns, 0).expect("mysql row");

        assert_eq!(
            row,
            vec![StandaloneMysqlValue::Bytes(
                br#"['{"2:3": null}']"#.to_vec()
            )]
        );
    }

    #[test]
    fn build_mysql_row_uses_arrow_field_metadata_for_opaque_binary() {
        let payload_field = field_with_logical_type(
            Field::new("payload", DataType::Binary, true),
            LogicalType::Hll,
        );
        let array = Arc::new(BinaryArray::from(vec![Some(b"opaque".as_slice())])) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![payload_field.clone()])),
            vec![Arc::clone(&array)],
        )
        .expect("batch");
        let chunk = Chunk::new_with_chunk_schema(
            batch,
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    payload_field,
                    None,
                    None,
                )])
                .expect("chunk schema"),
            ),
        );
        let columns = vec![QueryResultColumn {
            name: "payload".to_string(),
            data_type: array.data_type().clone(),
            nullable: true,
            logical_type: None,
        }];

        let row = build_mysql_row(&chunk, &columns, 0).expect("mysql row");

        assert_eq!(row, vec![StandaloneMysqlValue::Null]);
    }
}
