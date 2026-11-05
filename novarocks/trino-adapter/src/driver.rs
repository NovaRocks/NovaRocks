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
    error_mapping::{
        TrinoErrorSource, TrinoQueryError, query_error_for_query, query_error_for_query_with_effect,
    },
    paging::{PagePipeline, ResultSourceBacking},
    request::RequestSessionError,
    settlement::{Disposition, Settlement, SettlementOwner},
};
use bytes::Bytes;
use novarocks_query_application::{
    api::{ResultDelivery, StatementExternalEffectDisposition},
    cancellation::QueryCancellationReason,
    protocol_delivery::QuerySessionOutput,
    session::{
        QuerySession, QuerySessionFactory, StatementAdmission, StatementOutputShape,
        StatementSessionRequest,
    },
    session_control::GovernedStatementVisibilitySealOutcome,
    session_effects::SessionEffects,
    session_error::{QueryServiceError, QueryServiceErrorKind},
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

fn internal_with_effect(
    query_id: &str,
    message: &str,
    effect: Option<StatementExternalEffectDisposition>,
) -> TrinoQueryError {
    query_error_for_query_with_effect(
        query_id,
        TrinoErrorSource::Service {
            error: &QueryServiceError::new(QueryServiceErrorKind::Internal, message),
            sql: "",
        },
        effect,
    )
}
fn cancellation(disposition: Disposition) -> QueryCancellationReason {
    match disposition {
        Disposition::ClientCancel => QueryCancellationReason::ClientCancelRequested,
        _ => QueryCancellationReason::ClientDisconnected,
    }
}
fn cancel_error(query_id: &str, disposition: Disposition) -> TrinoQueryError {
    cancel_error_with_effect(query_id, disposition, None)
}
fn cancel_error_with_effect(
    query_id: &str,
    disposition: Disposition,
    effect: Option<StatementExternalEffectDisposition>,
) -> TrinoQueryError {
    let query_error_for_query = |id: &str, source: TrinoErrorSource<'_>| {
        query_error_for_query_with_effect(id, source, effect)
    };
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
        failed(internal_with_effect(
            entry.id.as_str(),
            "Trino query driver panicked",
            entry.external_effect_disposition(),
        )),
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
                RequestSessionError::QueryTextTooLarge(message) => query_error_for_query(
                    entry.id.as_str(),
                    TrinoErrorSource::QueryTextTooLarge { message: &message },
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
    let session_request =
        StatementSessionRequest::new(entry.id.clone(), request.principal(), value);
    let mut admission = session_request.admission().subscribe();
    let mut session = match sessions.open_statement_session(session_request) {
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
    let mut observing_admission = true;
    let statement = loop {
        tokio::select! {
            output = &mut execute => break output,
            changed = admission.changed(), if observing_admission => {
                if changed.is_err() { observing_admission = false; }
                let fact = *admission.borrow_and_update();
                if let Some(fact) = fact { admit(&entry, fact); }
            }
            _ = commands.changed() => {
                let disposition = (*commands.borrow()).unwrap_or(Disposition::Abandon);
                executing_session.cancel_current(cancellation(disposition));
                break execute.await;
            }
        }
    };
    let statement = match statement {
        Ok(statement) => statement,
        Err(error) => {
            entry.retain_external_effect(error.external_effect_disposition());
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
    let external_effect = output.external_effect_disposition();
    // Preserve this already received fact in the original entry before any
    // delivery metadata, renderer, framing or page operation can unwind.
    entry.retain_external_effect(external_effect);
    if let Some(fact) = *admission.borrow() {
        admit(&entry, fact);
    }
    #[cfg(test)]
    let delivery_panic = entry.delivery_panic.lock().unwrap().take();
    #[cfg(test)]
    if let Some((entered, release)) = delivery_panic {
        entered.wait().await;
        release.wait().await;
        panic!("controlled post-transfer delivery panic");
    }

    {
        let shape = match &output {
            QuerySessionOutput::GovernedCompletion(result) => Some(match result.completion() {
                novarocks_query_application::session_control::StatementCommandCompletion::Empty => {
                    StatementOutputShape::Empty
                }
                novarocks_query_application::session_control::StatementCommandCompletion::Rows(
                    _,
                ) => StatementOutputShape::Rows,
            }),
            QuerySessionOutput::Ok => None,
            QuerySessionOutput::GovernedQuery(_) | QuerySessionOutput::StreamingQuery(_) => {
                Some(StatementOutputShape::ResultSet)
            }
            _ => None,
        };
        if let Some(shape) = shape {
            let mut production = entry.production.lock().expect("query pipeline");
            if production
                .admission
                .is_some_and(|fact| fact.shape == StatementOutputShape::UndeterminedCall)
            {
                production.admission = Some(StatementAdmission { kind, shape });
            }
        }
    }
    let internal = |id: &str, message: &str| internal_with_effect(id, message, external_effect);
    let query_error_for_query = |id: &str, source: TrinoErrorSource<'_>| {
        query_error_for_query_with_effect(id, source, external_effect)
    };
    let (mut owner, produced) = match output {
        QuerySessionOutput::StreamingQuery(mut result) => {
            let produced = async {
                let schema = result.begin_schema().ok_or_else(|| internal(entry.id.as_str(), "stream has no original schema"))?;
                tracing::info!(
                    client_query_id = %entry.id,
                    query_id = %schema.query_id(),
                    "Trino statement logical query identity"
                );
                let render = schema.schema().client_render().ok_or_else(|| internal(entry.id.as_str(), "stream has no frozen client presentation"))?;
                initialize(&entry, render, result.result_window_alias().map_err(|_| internal(entry.id.as_str(), "statement result window is absent"))?, ResultSourceBacking::RootSegments, external_effect)?;
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
                            for span in rows.payload_spans() { append(&entry, span, &mut commands, external_effect).await?; }
                            segment.complete();
                        }
                        Some(ResultDelivery::End(end)) => {
                            let execution = end.execution_id();
                            tracing::info!(
                                client_query_id = %entry.id,
                                query_id = %execution.query_id(),
                                attempt_id = execution.attempt_id().get(),
                                query_execution_id = ?execution,
                                "Trino statement execution identity"
                            );
                            end.complete();
                            break;
                        }
                        None => return Err(internal(entry.id.as_str(), "stream closed before its original End").into()),
                    }
                }
                Ok(())
            }.await;
            (SettlementOwner::Streaming(result), produced)
        }
        QuerySessionOutput::GovernedQuery(result) => {
            let (local, owner) = result.into_parts();
            let produced = produce_local(&entry, local, &owner, request.sql(), &mut commands).await;
            (SettlementOwner::Governed(Box::new(owner)), produced)
        }
        QuerySessionOutput::GovernedCompletion(result) => {
            let (completion, local, protocol) = result.into_parts_with_rows();
            let produced = match (completion, local) {
                (
                    novarocks_query_application::session_control::StatementCommandCompletion::Empty,
                    None,
                ) => Ok(()),
                (
                    novarocks_query_application::session_control::StatementCommandCompletion::Rows(
                        _,
                    ),
                    Some(local),
                ) => {
                    entry
                        .production
                        .lock()
                        .expect("query pipeline")
                        .selected_count = completion.count();
                    produce_local(&entry, local, &protocol, request.sql(), &mut commands).await
                }
                _ => Err(internal(
                    entry.id.as_str(),
                    "completion has no original prepared count row",
                )
                .into()),
            };
            (SettlementOwner::Governed(Box::new(protocol)), produced)
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
        },
    );
}
fn initialize(
    entry: &QueryEntry,
    schema: &novarocks_result_contract::ClientRenderSchema,
    window: novarocks_workload_control::ResultWindowAlias,
    source: ResultSourceBacking,
    effect: Option<StatementExternalEffectDisposition>,
) -> Result<(), TrinoQueryError> {
    let internal = |id: &str, message: &str| internal_with_effect(id, message, effect);
    let query_error_for_query = |id: &str, source: TrinoErrorSource<'_>| {
        query_error_for_query_with_effect(id, source, effect)
    };
    let resource_error = |message: &str| {
        query_error_for_query(entry.id.as_str(), TrinoErrorSource::RowTooLarge { message })
    };
    let available = window
        .payload_capacity_bytes()
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
    if production
        .columns
        .as_ref()
        .is_some_and(|admitted| admitted != &columns)
    {
        return Err(internal(
            entry.id.as_str(),
            "prepared columns differ from their admitted static shape",
        ));
    }
    production.columns = Some(columns);
    production.pipeline = Some(pipeline);
    Ok(())
}
async fn append(
    entry: &QueryEntry,
    span: BorrowedPayloadSpan<'_>,
    commands: &mut watch::Receiver<Option<Disposition>>,
    effect: Option<StatementExternalEffectDisposition>,
) -> Result<(), DriverFailure> {
    let internal = |id: &str, message: &str| internal_with_effect(id, message, effect);
    let query_error_for_query = |id: &str, source: TrinoErrorSource<'_>| {
        query_error_for_query_with_effect(id, source, effect)
    };
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
    let effect = settlement.owner.external_effect_disposition();
    let (outcome, actual_reason) = settlement.finish(requested);
    let (disposition, error) =
        command_receipt(entry.id.as_str(), requested, outcome, actual_reason, effect);
    let mut result = failed(error);
    result.disposition = disposition;
    publish(entry, result);
}
pub(crate) fn command_receipt(
    query_id: &str,
    requested: Disposition,
    outcome: novarocks_query_application::session_control::GovernedStatementFinishOutcome,
    actual_reason: Option<QueryCancellationReason>,
    effect: Option<StatementExternalEffectDisposition>,
) -> (Disposition, TrinoQueryError) {
    let internal = |id: &str, message: &str| internal_with_effect(id, message, effect);
    let query_error_for_query = |id: &str, source: TrinoErrorSource<'_>| {
        query_error_for_query_with_effect(id, source, effect)
    };
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
        Finish::ProtocolFailed => (
            requested,
            cancel_error_with_effect(query_id, requested, effect),
        ),
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
    effect: Option<StatementExternalEffectDisposition>,
) -> DriverFailure {
    let query_error_for_query = |id: &str, source: TrinoErrorSource<'_>| {
        query_error_for_query_with_effect(id, source, effect)
    };
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

// These immutable metadata bytes are the projection of the admitted static
// shape, not a second row encoder. The actual data always uses the Local graph.
fn admit(entry: &QueryEntry, fact: StatementAdmission) {
    let mut production = entry.production.lock().expect("query pipeline");
    if let Some(original) = production.admission {
        assert_eq!(original, fact);
        return;
    }
    production.columns = match fact.shape {
        StatementOutputShape::Empty => Some(Bytes::from_static(b"[]")),
        StatementOutputShape::Rows => Some(Bytes::from_static(br#"[{"name":"rows","type":"bigint","typeSignature":{"rawType":"bigint","arguments":[]}}]"#)),
        StatementOutputShape::ResultSet | StatementOutputShape::UndeterminedCall => None,
    };
    production.admission = Some(fact);
    drop(production);
    entry.progress.notify_waiters();
}

async fn produce_local(
    entry: &QueryEntry,
    local: novarocks_query_application::api::OwnedLocalResult,
    owner: &novarocks_query_application::protocol_delivery::GovernedProtocolOwner,
    sql: &str,
    commands: &mut watch::Receiver<Option<Disposition>>,
) -> Result<(), DriverFailure> {
    let effect = owner.external_effect_disposition();
    let internal = |id: &str, message: &str| internal_with_effect(id, message, effect);
    let mut cursor = local.into_cursor();
    let mut bytes = vec![0; RootProfileV1::EMIT_BYTES_PER_TURN];
    let retained_bytes = cursor
        .retained_backing_bytes()
        .checked_add(bytes.capacity() as u64)
        .ok_or_else(|| internal(entry.id.as_str(), "Local delivery backing overflows"))?;
    initialize(
        entry,
        cursor.client_render(),
        owner
            .result_window_alias()
            .map_err(|_| internal(entry.id.as_str(), "local result window is absent"))?,
        ResultSourceBacking::Local { retained_bytes },
        effect,
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
            .map_err(|error| local_render_error(entry, sql, &error, effect))?;
        if turn.emitted_bytes != 0 {
            let body = frontier
                .validate_body(profile, &bytes[..turn.emitted_bytes])
                .map_err(|_| internal(entry.id.as_str(), "local row framing is invalid"))?;
            for span in body.payload_spans() {
                append(entry, span, commands, effect).await?;
            }
            frontier = body.after();
        }
        if turn.status == RenderTurnStatus::InputComplete {
            break;
        }
        tokio::task::yield_now().await;
    }
    frontier
        .validate_end()
        .map_err(|_| internal(entry.id.as_str(), "local row stream ends inside a row"))?;
    Ok(())
}
