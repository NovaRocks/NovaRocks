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

use crate::{
    columns::{columns_backing_bytes, columns_with_backing_limit},
    entry::{Outcome, QueryEntry},
    error_mapping::{TrinoErrorSource, TrinoQueryError, query_error_for_query},
    paging::{PagePipeline, ResultSourceBacking},
    request::RequestSessionError,
    settlement::{Disposition, Settlement, SettlementOwner},
};
use bytes::Bytes;
use novarocks_query_application::{
    api::ResultDelivery,
    cancellation::QueryCancellationReason,
    protocol_delivery::QuerySessionOutput,
    session::{QuerySession, QuerySessionFactory, StatementSessionRequest},
    session_control::GovernedStatementVisibilitySealOutcome,
    session_effects::SessionEffects,
    session_error::{QueryServiceError, QueryServiceErrorKind},
    statement_kind::StatementKind,
};
use novarocks_result_contract::{
    BorrowedPayloadSpan, ClientRowProfile, ClientRowStreamCursor, RootProfileV1,
};
use novarocks_result_render::RenderTurnStatus;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

enum DriverFailure {
    Originating(TrinoQueryError),
    ProtocolCommand(Disposition),
}
impl From<TrinoQueryError> for DriverFailure {
    fn from(error: TrinoQueryError) -> Self {
        Self::Originating(error)
    }
}

pub(crate) fn internal(query_id: &str, message: &str) -> TrinoQueryError {
    query_error_for_query(
        query_id,
        TrinoErrorSource::Service {
            error: &QueryServiceError::new(QueryServiceErrorKind::Internal, message),
            sql: "",
        },
    )
}
fn cancellation(disposition: Disposition) -> QueryCancellationReason {
    match disposition {
        Disposition::ClientCancel => QueryCancellationReason::ClientCancelRequested,
        _ => QueryCancellationReason::ClientDisconnected,
    }
}
fn cancel_error(query_id: &str, disposition: Disposition) -> TrinoQueryError {
    query_error_for_query(
        query_id,
        TrinoErrorSource::Cancellation {
            reason: &cancellation(disposition),
            message: if disposition == Disposition::ClientCancel {
                "Query canceled by client"
            } else {
                "Query abandoned by client"
            },
        },
    )
}
fn publish(entry: &QueryEntry, outcome: Outcome) {
    entry.production.lock().expect("query pipeline").outcome = Some(outcome);
    entry.progress.notify_waiters();
}
pub(crate) fn publish_panic(entry: &QueryEntry) {
    publish(
        entry,
        failed(internal(entry.id.as_str(), "Trino query driver panicked")),
    );
}
fn command_outcome(query_id: &str, disposition: Disposition) -> Outcome {
    let mut outcome = failed(cancel_error(query_id, disposition));
    outcome.disposition = disposition;
    outcome
}
fn failed(error: TrinoQueryError) -> Outcome {
    Outcome {
        settlement: None,
        error: Some(error),
        disposition: Disposition::Fail,
        effects: SessionEffects::default(),
        kind: StatementKind::Empty,
    }
}
struct SessionClose(Option<Arc<dyn QuerySession>>);
impl Drop for SessionClose {
    fn drop(&mut self) {
        if let Some(session) = self.0.as_ref() {
            session.close();
        }
    }
}

/// The stable entry owns this task; request cancellation never drops its future.
pub(crate) async fn run(
    entry: Arc<QueryEntry>,
    sessions: Arc<dyn QuerySessionFactory>,
    request: crate::request::ParsedStatementRequest,
) {
    let mut commands = entry.command.subscribe();
    let initial_command = *commands.borrow();
    if let Some(disposition) = initial_command {
        publish(&entry, command_outcome(entry.id.as_str(), disposition));
        return;
    }
    let value = match request.session_value() {
        Ok(value) => value,
        Err(error) => {
            let error = match error {
                RequestSessionError::Unsupported(message) => query_error_for_query(
                    entry.id.as_str(),
                    TrinoErrorSource::Unsupported { message: &message },
                ),
                RequestSessionError::Session(error) => query_error_for_query(
                    entry.id.as_str(),
                    TrinoErrorSource::Session {
                        error: &error,
                        sql: request.sql(),
                    },
                ),
            };
            publish(&entry, failed(error));
            return;
        }
    };
    let mut session = match sessions.open_statement_session(StatementSessionRequest::new(
        entry.id.clone(),
        request.principal(),
        value,
    )) {
        Ok(session) => SessionClose(Some(session)),
        Err(error) => {
            publish(
                &entry,
                failed(query_error_for_query(
                    entry.id.as_str(),
                    TrinoErrorSource::Session {
                        error: &error,
                        sql: request.sql(),
                    },
                )),
            );
            return;
        }
    };
    let before_execute = *commands.borrow();
    if let Some(disposition) = before_execute {
        publish(&entry, command_outcome(entry.id.as_str(), disposition));
        return;
    }
    let executing_session = Arc::clone(session.0.as_ref().expect("original session"));
    let execute = executing_session.execute_statement(request.sql());
    tokio::pin!(execute);
    let statement = tokio::select! {
        output = &mut execute => output,
        _ = commands.changed() => {
            let disposition = (*commands.borrow()).unwrap_or(Disposition::Abandon);
            executing_session.cancel_current(cancellation(disposition));
            execute.await
        }
    };
    let statement = match statement {
        Ok(statement) => statement,
        Err(error) => {
            publish(
                &entry,
                failed(query_error_for_query(
                    entry.id.as_str(),
                    TrinoErrorSource::Service {
                        error: &error,
                        sql: request.sql(),
                    },
                )),
            );
            return;
        }
    };
    let (output, terminal, effects, kind) = statement.into_parts_with_session();
    let (mut owner, produced) = match output {
        QuerySessionOutput::StreamingQuery(mut result) => {
            let produced = async {
                let schema = result.begin_schema().ok_or_else(|| internal(entry.id.as_str(), "stream has no original schema"))?;
                let render = schema.schema().client_render().ok_or_else(|| internal(entry.id.as_str(), "stream has no frozen client presentation"))?;
                initialize(&entry, render, result.result_window_alias().map_err(|_| internal(entry.id.as_str(), "statement result window is absent"))?, ResultSourceBacking::RootSegments)?;
                schema.complete();
                loop {
                    if let Some(disposition) = *commands.borrow() { return Err(DriverFailure::ProtocolCommand(disposition)); }
                    let delivery = tokio::select! {
                        delivery = result.next_delivery() => delivery.map_err(|error| query_error_for_query(entry.id.as_str(), TrinoErrorSource::Execution { error: &error, sql: request.sql() }))?,
                        _ = commands.changed() => return Err(DriverFailure::ProtocolCommand((*commands.borrow()).unwrap_or(Disposition::Abandon))),
                    };
                    match delivery {
                        Some(ResultDelivery::Segment(segment)) => {
                            let rows = segment.client_rows().ok_or_else(|| internal(entry.id.as_str(), "stream is not a client-row carrier"))?;
                            for span in rows.payload_spans() { append(&entry, span, &mut commands).await?; }
                            segment.complete();
                        }
                        Some(ResultDelivery::End(end)) => { end.complete(); break; }
                        None => return Err(internal(entry.id.as_str(), "stream closed before its original End").into()),
                    }
                }
                Ok(())
            }.await;
            (SettlementOwner::Streaming(result), produced)
        }
        QuerySessionOutput::GovernedQuery(result) => {
            let (local, owner) = result.into_parts();
            let produced = async {
                let mut cursor = local.into_cursor();
                let mut bytes = vec![0; RootProfileV1::EMIT_BYTES_PER_TURN];
                let retained_bytes = cursor
                    .retained_backing_bytes()
                    .checked_add(bytes.capacity() as u64)
                    .ok_or_else(|| {
                        internal(entry.id.as_str(), "Local delivery backing overflows")
                    })?;
                initialize(
                    &entry,
                    cursor.client_render(),
                    owner.result_window_alias().map_err(|_| {
                        internal(entry.id.as_str(), "local result window is absent")
                    })?,
                    ResultSourceBacking::Local { retained_bytes },
                )?;
                let mut frontier = ClientRowStreamCursor::new();
                let profile = ClientRowProfile::try_new(
                    RootProfileV1::SEGMENT_BYTES,
                    RootProfileV1::ROW_PAYLOAD_BYTES,
                )
                .expect("frozen profile");
                loop {
                    if let Some(disposition) = *commands.borrow() {
                        return Err(DriverFailure::ProtocolCommand(disposition));
                    }
                    let turn = cursor
                        .step(&mut bytes)
                        .map_err(|error| local_render_error(&entry, request.sql(), &error))?;
                    if turn.emitted_bytes != 0 {
                        let body = frontier
                            .validate_body(profile, &bytes[..turn.emitted_bytes])
                            .map_err(|_| {
                                internal(entry.id.as_str(), "local row framing is invalid")
                            })?;
                        for span in body.payload_spans() {
                            append(&entry, span, &mut commands).await?;
                        }
                        frontier = body.after();
                    }
                    if turn.status == RenderTurnStatus::InputComplete {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                frontier.validate_end().map_err(|_| {
                    internal(entry.id.as_str(), "local row stream ends inside a row")
                })?;
                Ok(())
            }
            .await;
            (SettlementOwner::Governed(Box::new(owner)), produced)
        }
        QuerySessionOutput::GovernedCompletion(result) => {
            let (_completion, protocol) = result.into_parts();
            (SettlementOwner::Governed(Box::new(protocol)), Ok(()))
        }
        QuerySessionOutput::GovernedError(result) => {
            let (error, owner) = result.into_parts();
            (
                SettlementOwner::Governed(Box::new(owner)),
                Err(DriverFailure::Originating(query_error_for_query(
                    entry.id.as_str(),
                    TrinoErrorSource::Service {
                        error: &error,
                        sql: request.sql(),
                    },
                ))),
            )
        }
        QuerySessionOutput::Ok => (SettlementOwner::Ungoverned, Ok(())),
        QuerySessionOutput::Query(_) => (
            SettlementOwner::Ungoverned,
            Err(DriverFailure::Originating(internal(
                entry.id.as_str(),
                "application-local result lacks its governed owner",
            ))),
        ),
    };
    let pending = *commands.borrow();
    let produced = match produced {
        Err(DriverFailure::ProtocolCommand(disposition)) => {
            finish_command(
                &entry,
                Settlement {
                    owner,
                    terminal,
                    session: session.0.take(),
                },
                disposition,
            );
            return;
        }
        Ok(()) if pending.is_some() => {
            finish_command(
                &entry,
                Settlement {
                    owner,
                    terminal,
                    session: session.0.take(),
                },
                pending.expect("observed command"),
            );
            return;
        }
        Ok(()) => Ok(()),
        Err(DriverFailure::Originating(error)) => Err(error),
    };
    let produced = produced.and_then(|()| {
        let production = entry.production.lock().expect("query pipeline");
        if production
            .pipeline
            .as_ref()
            .is_some_and(|pipe| !pipe.at_row_boundary())
        {
            Err(internal(
                entry.id.as_str(),
                "client row stream ended inside a row",
            ))
        } else {
            Ok(())
        }
    });
    let (error, disposition) = match produced {
        Err(error) => (Some(error), Disposition::Fail),
        Ok(()) => match owner.seal() {
            GovernedStatementVisibilitySealOutcome::Sealed => (None, Disposition::Complete),
            GovernedStatementVisibilitySealOutcome::Cancelled(_) => {
                // The stable original scope supplies the actual cancellation class.
                let error = match owner.cancellation_reason() {
                    Some(reason) => query_error_for_query(
                        entry.id.as_str(),
                        TrinoErrorSource::Cancellation {
                            reason: &reason,
                            message: "Statement canceled before success visibility",
                        },
                    ),
                    None => internal(
                        entry.id.as_str(),
                        "sealed cancellation has no original cause",
                    ),
                };
                (Some(error), Disposition::Cancelled)
            }
            GovernedStatementVisibilitySealOutcome::Failed
            | GovernedStatementVisibilitySealOutcome::Stale => (
                Some(internal(
                    entry.id.as_str(),
                    "statement could not seal success visibility",
                )),
                Disposition::Fail,
            ),
        },
    };
    publish(
        &entry,
        Outcome {
            settlement: Some(Settlement {
                owner,
                terminal,
                session: session.0.take(),
            }),
            error,
            disposition,
            effects,
            kind,
        },
    );
}
fn initialize(
    entry: &QueryEntry,
    schema: &novarocks_result_contract::ClientRenderSchema,
    window: novarocks_workload_control::ResultWindowAlias,
    source: ResultSourceBacking,
) -> Result<(), TrinoQueryError> {
    let resource_error = |message: &str| {
        query_error_for_query(entry.id.as_str(), TrinoErrorSource::RowTooLarge { message })
    };
    let available = window
        .all_objects_bytes()
        .map_err(|_| {
            internal(
                entry.id.as_str(),
                "metadata has no original window envelope",
            )
        })?
        .checked_sub(source.retained_bytes())
        .and_then(|bytes| {
            bytes.checked_sub((2 * crate::paging::PAGE_ENVELOPE_RESERVE_BYTES + 1) as u64)
        })
        .ok_or_else(|| {
            resource_error("Original result window cannot hold column metadata and a page")
        })?;
    let available = usize::try_from(available)
        .map_err(|_| resource_error("Column metadata backing overflows"))?;
    let columns = columns_with_backing_limit(schema, available).map_err(|error| {
        if error.is_backing_failure() {
            resource_error("Column metadata exceeds the original result window")
        } else {
            internal(
                entry.id.as_str(),
                "result presentation is not a frozen Trino schema",
            )
        }
    })?;
    let graph = columns_backing_bytes(&columns)
        .map_err(|_| resource_error("Column metadata backing overflows"))?;
    let wire_limit = available.checked_sub(graph).ok_or_else(|| {
        resource_error("Column metadata construction exceeds the original result window")
    })?;
    let wire = crate::json_bytes::exact_json_bytes(&columns, wire_limit).map_err(|_| {
        resource_error("Column metadata serialization exceeds the original result window")
    })?;
    // Metadata construction is over before the full page backing is allocated.
    drop(columns);
    let columns = Bytes::from(wire);
    let pipeline = PagePipeline::new(window, columns.len(), source).map_err(|message| {
        query_error_for_query(
            entry.id.as_str(),
            TrinoErrorSource::RowTooLarge { message: &message },
        )
    })?;
    let mut production = entry.production.lock().expect("query pipeline");
    production.columns = Some(columns);
    production.pipeline = Some(pipeline);
    Ok(())
}
async fn append(
    entry: &QueryEntry,
    span: BorrowedPayloadSpan<'_>,
    commands: &mut watch::Receiver<Option<Disposition>>,
) -> Result<(), DriverFailure> {
    loop {
        if let Some(disposition) = *commands.borrow() {
            return Err(DriverFailure::ProtocolCommand(disposition));
        }
        {
            let mut production = entry.production.lock().expect("query pipeline");
            let retained = production.retained;
            let pipe = production.pipeline.as_mut().expect("initialized pipeline");
            if let Some(length) = span.starts_row
                && length.get() as usize > pipe.row_limit()
            {
                return Err(query_error_for_query(
                    entry.id.as_str(),
                    TrinoErrorSource::RowTooLarge {
                        message: "Trino row exceeds the original result window",
                    },
                )
                .into());
            }
            let row_fits = span
                .starts_row
                .is_none_or(|n| pipe.can_start(n.get() as usize));
            if !retained && !pipe.ready() && row_fits && pipe.reclaim() {
                pipe.append(span)
                    .map_err(|message| internal(entry.id.as_str(), &message))?;
                entry.progress.notify_waiters();
                return Ok(());
            }
        }
        entry.progress.notify_waiters();
        let notified = entry.progress.notified();
        tokio::select! {
            _ = notified => {},
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}, // actual body aliases may exit after ACK
            _ = commands.changed() => {},
        }
    }
}

fn finish_command(entry: &QueryEntry, settlement: Settlement, requested: Disposition) {
    entry
        .production
        .lock()
        .expect("query pipeline")
        .pipeline
        .take();
    entry
        .production
        .lock()
        .expect("query pipeline")
        .columns
        .take();
    let (outcome, actual_reason) = settlement.finish(requested);
    let (disposition, error) =
        command_receipt(entry.id.as_str(), requested, outcome, actual_reason);
    let mut result = failed(error);
    result.disposition = disposition;
    publish(entry, result);
}
pub(crate) fn command_receipt(
    query_id: &str,
    requested: Disposition,
    outcome: novarocks_query_application::session_control::GovernedStatementFinishOutcome,
    actual_reason: Option<QueryCancellationReason>,
) -> (Disposition, TrinoQueryError) {
    use novarocks_query_application::session_control::GovernedStatementFinishOutcome as Finish;
    match outcome {
        Finish::Cancelled(_) => match actual_reason {
            Some(reason) => {
                let disposition = match reason {
                    QueryCancellationReason::ClientCancelRequested => Disposition::ClientCancel,
                    QueryCancellationReason::ClientDisconnected => Disposition::Abandon,
                    _ => Disposition::Cancelled,
                };
                (
                    disposition,
                    query_error_for_query(
                        query_id,
                        TrinoErrorSource::Cancellation {
                            reason: &reason,
                            message: "Statement canceled by its original accepted cause",
                        },
                    ),
                )
            }
            None => (
                Disposition::Fail,
                internal(query_id, "canceled settlement has no original cause"),
            ),
        },
        Finish::ProtocolFailed => (requested, cancel_error(query_id, requested)),
        Finish::Completed | Finish::Stale => (
            Disposition::Fail,
            internal(
                query_id,
                "cancel command did not settle its original protocol owner",
            ),
        ),
    }
}

fn local_render_error(
    entry: &QueryEntry,
    sql: &str,
    error: &novarocks_result_render::RenderError,
) -> DriverFailure {
    if error.kind == novarocks_result_render::RenderErrorKind::RowTooLarge {
        return query_error_for_query(
            entry.id.as_str(),
            TrinoErrorSource::RowTooLarge {
                message: "Local client row exceeds its frozen payload bound",
            },
        )
        .into();
    }
    use novarocks_execution_contract::failure::FailureProjection;
    let execution = novarocks_query_application::api::QueryExecutionError::from_execution(
        novarocks_query_application::execution_error::ExecutionFailureInput::new(
            error.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating(&error.to_string()),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator,
        ),
    );
    query_error_for_query(
        entry.id.as_str(),
        TrinoErrorSource::Execution {
            error: &execution,
            sql,
        },
    )
    .into()
}
