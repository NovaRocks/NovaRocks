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

//! Render a closed Local graph through the same finite MySQL framing writer.

use crate::governed_result_writer::{
    MysqlStatementWriteOutcome, cancelled_query_result_delivery as cancelled,
    governed_cancelled_query_result_delivery, is_terminal_cancellation,
};
use crate::relay_result_writer::{
    CLOSING_DEADLINE, CLOSING_OBJECT_BYTES, error_payload, invalid, io_error, resident_tail,
    success_payload, timeout_error, write_body,
};
use novarocks_query_application::{
    api::{LocalColumnKind, LocalRenderCursor, OwnedLocalResult, QueryExecutionError},
    cancellation::QueryCancellationReason,
    protocol_delivery::{GovernedImmediateStatementResult, GovernedProtocolOwner},
    session_control::GovernedStatementVisibilitySealOutcome,
};
use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor, RootProfileV1};
use novarocks_result_render::RenderTurnStatus;
use novarocks_workload_control::ResultWindowAlias;
use opensrv_mysql::{Column, ColumnFlags, ColumnType, QueryResultWriter, StreamingResponseLease};
use std::{io, sync::Arc};
use tokio::{io::AsyncWrite, time::Instant};

/// Destruction order is the backing/physical-exit proof, including unwind and
/// cancellation of the enclosing async writer. No graph or segment can escape.
struct LocalProtocolBuffers {
    cursor: LocalRenderCursor,
    bytes: Vec<u8>,
    metadata: Option<opensrv_mysql::FrozenMetadata>,
    _window: ResultWindowAlias,
}

pub(crate) async fn write_local_result_one<'writer, W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let (graph, mut protocol) = result.into_parts();
    let limits = results.protocol_limits();
    let capabilities = results.client_capabilities();
    if results.is_binary() || limits.row_bytes < RootProfileV1::ROW_PAYLOAD_BYTES as usize {
        drop(graph);
        return close_initial(
            protocol,
            results,
            invalid("Local delivery requires the supported text-row profile"),
        )
        .await;
    }
    let mut budget = crate::active_delivery_budget::ActiveDeliveryWorkBudget::new();
    // The complete producer window was admitted before the source callback.
    // Metadata, render scratch and socket work retain that same window.
    let cancellation = protocol.cancellation();
    if let Some(reason) = cancellation.reason() {
        drop(graph);
        return close_initial(protocol, results, cancelled(reason)).await;
    }
    let metadata = budget.run_sync(|| local_metadata(&graph, capabilities, limits));
    let metadata = match metadata {
        Ok(metadata) => metadata,
        Err(error) => {
            drop(graph);
            if error.kind() == io::ErrorKind::TimedOut {
                drop(results);
                let _ = protocol.client_disconnected();
                return Err(error);
            }
            return close_initial(protocol, results, invalid(error.to_string())).await;
        }
    };
    let mut buffers = match budget.run_sync(|| {
        let cursor = graph.into_cursor();
        Ok(LocalProtocolBuffers {
            _window: cursor.retain_physical_guard(),
            cursor,
            bytes: vec![0; RootProfileV1::SEGMENT_BYTES],
            metadata: Some(metadata),
        })
    }) {
        Ok(buffers) => buffers,
        Err(error) => {
            drop(results);
            let _ = protocol.client_disconnected();
            return Err(error);
        }
    };
    // From this point all socket/coalescer/metadata work is finite and the
    // actual writer task retains the same producer window throughout.
    let mut lease = match budget.run(results.into_streaming_result()).await {
        Ok(lease) => lease,
        Err(error) => {
            drop(buffers);
            let _ = protocol.client_disconnected();
            return Err(error);
        }
    };
    let cancellation = protocol.cancellation();
    let started = tokio::select! {
        biased;
        reason = cancellation.cancelled() => Err(LocalWriteInterruption::Query(Box::new(cancelled(reason)))),
        outcome = budget.run(async {
            lease.writer().start_metadata(buffers.metadata.take().expect("frozen Local metadata"))?;
            lease.writer().finish_metadata().await
        }) => outcome.map_err(LocalWriteInterruption::Io),
    };
    if let Err(error) = started {
        return interrupt(protocol, lease, buffers, error, None).await;
    }
    let profile = local_profile();
    let mut frontier = ClientRowStreamCursor::new();
    loop {
        if let Some(reason) = protocol.cancellation().reason() {
            return close_local(protocol, lease, buffers, cancelled(reason), None).await;
        }
        let turn = match budget.run_sync(|| {
            buffers
                .cursor
                .step(&mut buffers.bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
        }) {
            Ok(turn) => turn,
            Err(error) => {
                if error.kind() == io::ErrorKind::TimedOut {
                    return interrupt(
                        protocol,
                        lease,
                        buffers,
                        LocalWriteInterruption::Io(error),
                        None,
                    )
                    .await;
                }
                return close_local(protocol, lease, buffers, invalid(error.to_string()), None)
                    .await;
            }
        };
        // Cancellation observed after a counting/render turn prevents its
        // publication. An unfinished previously written row has no resident
        // tail in this case, so it must disconnect without further encoding.
        if let Some(reason) = protocol.cancellation().reason() {
            return close_local(protocol, lease, buffers, cancelled(reason), None).await;
        }
        if turn.emitted_bytes != 0 {
            let body = match budget.run_sync(|| {
                frontier
                    .validate_body(profile, &buffers.bytes[..turn.emitted_bytes])
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
            }) {
                Ok(body) => body,
                Err(error) => {
                    if error.kind() == io::ErrorKind::TimedOut {
                        return interrupt(
                            protocol,
                            lease,
                            buffers,
                            LocalWriteInterruption::Io(error),
                            None,
                        )
                        .await;
                    }
                    return close_local(protocol, lease, buffers, invalid(error.to_string()), None)
                        .await;
                }
            };
            if lease.receipt().logical_remaining() != body.before().remaining() as usize {
                drop(lease);
                drop(buffers);
                let _ = protocol.fail();
                return Err(io_error(invalid(
                    "Local framing differs from its validated cursor",
                )));
            }
            let next = body.after();
            let cancellation = protocol.cancellation();
            let written = tokio::select! {
                biased;
                reason = cancellation.cancelled() => Err(LocalWriteInterruption::Query(Box::new(cancelled(reason)))),
                outcome = budget.run(write_body(lease.writer(), &body)) => outcome.map_err(LocalWriteInterruption::Io),
            };
            if let Err(error) = written {
                return interrupt(
                    protocol,
                    lease,
                    buffers,
                    error,
                    Some((frontier, turn.emitted_bytes)),
                )
                .await;
            }
            frontier = next;
        }
        if turn.status == RenderTurnStatus::InputComplete {
            if let Err(error) = frontier.validate_end() {
                drop(lease);
                drop(buffers);
                let _ = protocol.fail();
                return Err(io_error(invalid(error.to_string())));
            }
            match protocol.seal_success_visibility() {
                GovernedStatementVisibilitySealOutcome::Sealed => (),
                GovernedStatementVisibilitySealOutcome::Cancelled(reason) => {
                    return close_local(
                        protocol,
                        lease,
                        buffers,
                        governed_cancelled_query_result_delivery(reason),
                        None,
                    )
                    .await;
                }
                _ => {
                    return close_local(
                        protocol,
                        lease,
                        buffers,
                        invalid("Local result lost its success visibility generation"),
                        None,
                    )
                    .await;
                }
            }
            let written = budget
                .run(async {
                    let terminal = success_payload(capabilities, more_results);
                    lease.finish(&terminal).await
                })
                .await;
            // The segment, encoder/schema and legacy protection precede the
            // original statement grant even on a failed terminal write.
            drop(buffers);
            return match written {
                Ok(writer) => {
                    let _ = protocol.complete();
                    Ok(MysqlStatementWriteOutcome::Continue(writer))
                }
                Err(error) => {
                    let _ = protocol.client_disconnected();
                    Err(error)
                }
            };
        }
        // Empty batches, length counting and every emitted turn yield before
        // continuing. No unbounded scan or row build runs in one actor poll.
        tokio::task::yield_now().await;
    }
}

fn local_profile() -> ClientRowProfile {
    ClientRowProfile::try_new(
        RootProfileV1::SEGMENT_BYTES,
        RootProfileV1::ROW_PAYLOAD_BYTES,
    )
    .expect("frozen Local row profile")
}
fn local_metadata(
    graph: &OwnedLocalResult,
    capabilities: opensrv_mysql::CapabilityFlags,
    limits: opensrv_mysql::ProtocolLimits,
) -> io::Result<opensrv_mysql::FrozenMetadata> {
    // Borrowed preflight precedes every adapter String/Column copy.
    crate::relay_metadata::preflight_column_names(
        graph.columns().map(|column| column.name),
        capabilities,
        limits,
    )?;
    let columns = graph
        .columns()
        .map(|column| Column {
            table: String::new(),
            column: column.name.to_owned(),
            coltype: match column.kind {
                LocalColumnKind::Utf8 => ColumnType::MYSQL_TYPE_VAR_STRING,
                LocalColumnKind::Boolean => ColumnType::MYSQL_TYPE_TINY,
                LocalColumnKind::Int32 => ColumnType::MYSQL_TYPE_LONG,
                LocalColumnKind::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
            },
            colflags: if column.nullable {
                ColumnFlags::empty()
            } else {
                ColumnFlags::NOT_NULL_FLAG
            },
        })
        .collect::<Vec<_>>();
    crate::relay_metadata::frozen_result_metadata(&columns, capabilities, limits)
}
enum LocalWriteInterruption {
    Query(Box<QueryExecutionError>),
    Io(io::Error),
}
async fn interrupt<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    lease: StreamingResponseLease<'writer, W>,
    buffers: LocalProtocolBuffers,
    error: LocalWriteInterruption,
    current: Option<(ClientRowStreamCursor, usize)>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    match error {
        LocalWriteInterruption::Query(error) => {
            close_local(protocol, lease, buffers, *error, current).await
        }
        LocalWriteInterruption::Io(error) => {
            drop(lease);
            drop(buffers);
            let _ = protocol.client_disconnected();
            Err(error)
        }
    }
}
async fn close_initial<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'writer, W>,
    error: QueryExecutionError,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    if matches!(
        protocol.cancellation().reason(),
        Some(
            QueryCancellationReason::ExplicitKillConnection { .. }
                | QueryCancellationReason::ServerShutdown
                | QueryCancellationReason::ClientDisconnected
        )
    ) {
        let _ = protocol.client_disconnected();
        return Err(io_error(error));
    }
    let capacity = match protocol.try_closing_capacity(
        is_terminal_cancellation(&error) && protocol.cancellation().is_cancelled(),
    ) {
        Ok(capacity) if capacity.check_backing_total(CLOSING_OBJECT_BYTES).is_ok() => capacity,
        _ => {
            let _ = protocol.client_disconnected();
            return Err(io_error(error));
        }
    };
    let deadline = Instant::now() + CLOSING_DEADLINE;
    // A preceding negotiated OK may still need flushing. Move that exact
    // writer/future into Closing before polling it, so a slow preceding
    // terminator cannot retain this statement's ordinary Local position.
    let writer = InitialLocalClosingWriter {
        writer: Some(results),
        error,
    };
    let mut closing = match protocol.into_closing_delivery(writer, capacity, CLOSING_OBJECT_BYTES) {
        Ok(closing) => closing,
        Err((mut protocol, writer, _capacity)) => {
            drop(writer);
            let _ = protocol.client_disconnected();
            return Err(io_error(invalid(
                "initial Local closing lost its exact owner",
            )));
        }
    };
    match tokio::time::timeout_at(deadline, closing.writer_mut().finish()).await {
        Ok(Ok(writer)) => {
            let _ = closing.settle_after_writer_exit().await;
            writer.no_more_results().await?;
            Ok(MysqlStatementWriteOutcome::Terminated)
        }
        Ok(Err(error)) => {
            drop(closing);
            Err(error)
        }
        Err(_) => {
            drop(closing);
            Err(timeout_error())
        }
    }
}
struct InitialLocalClosingWriter<'writer, W: AsyncWrite + Unpin> {
    writer: Option<QueryResultWriter<'writer, W>>,
    error: QueryExecutionError,
}
impl<'writer, W: AsyncWrite + Unpin> InitialLocalClosingWriter<'writer, W> {
    async fn finish(&mut self) -> io::Result<QueryResultWriter<'writer, W>> {
        let mut lease = self
            .writer
            .take()
            .expect("initial closing owns its writer")
            .into_streaming_result()
            .await?;
        let kind = crate::error_mapping::execution_error_kind(&self.error);
        let payload = error_payload(
            &self.error,
            lease.writer().protocol_limits().diagnostic_bytes,
        );
        let mut writer = match lease.into_closing_lease(Vec::new(), kind, &payload[9..]) {
            Ok(writer) => writer,
            Err((lease, rejection)) => {
                drop(lease);
                return Err(rejection);
            }
        };
        drop(payload);
        writer.finish().await
    }
}

async fn close_local<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    mut lease: StreamingResponseLease<'writer, W>,
    mut buffers: LocalProtocolBuffers,
    error: QueryExecutionError,
    current: Option<(ClientRowStreamCursor, usize)>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    buffers.cursor.cancel();
    if matches!(
        protocol.cancellation().reason(),
        Some(
            QueryCancellationReason::ExplicitKillConnection { .. }
                | QueryCancellationReason::ServerShutdown
                | QueryCancellationReason::ClientDisconnected
        )
    ) {
        drop(lease);
        drop(buffers);
        let _ = protocol.client_disconnected();
        return Err(io_error(error));
    }
    let capacity = match protocol.try_closing_capacity(
        is_terminal_cancellation(&error) && protocol.cancellation().is_cancelled(),
    ) {
        Ok(capacity) if capacity.check_backing_total(CLOSING_OBJECT_BYTES).is_ok() => capacity,
        _ => {
            drop(lease);
            drop(buffers);
            let _ = protocol.client_disconnected();
            return Err(io_error(error));
        }
    };
    let body = current.and_then(|(frontier, length)| {
        frontier
            .validate_body(local_profile(), &buffers.bytes[..length])
            .ok()
    });
    let resident = resident_tail(
        lease.receipt(),
        lease.writer().buffered_row_bytes(),
        body.as_ref(),
        None,
    );
    let Some(resident) = resident else {
        drop(lease);
        drop(buffers);
        let _ = protocol.client_disconnected();
        return Err(io_error(error));
    };
    // Only the current row survives. New independent capacity covers the
    // compact copy and the still-live ordinary objects during this transfer.
    let size = resident.iter().map(|part| part.len()).sum();
    let mut bytes = Vec::with_capacity(size);
    for part in resident {
        bytes.extend_from_slice(part);
    }
    let mut tail = Vec::with_capacity(1);
    if !bytes.is_empty() {
        tail.push(opensrv_mysql::ResidentTailPart::new(
            Arc::from(bytes),
            0..size,
        )?);
    }
    let payload = error_payload(&error, lease.writer().protocol_limits().diagnostic_bytes);
    let kind = crate::error_mapping::execution_error_kind(&error);
    let writer = match lease.into_closing_lease(tail, kind, &payload[9..]) {
        Ok(writer) => writer,
        Err((lease, error)) => {
            drop(payload);
            drop(lease);
            drop(buffers);
            let _ = protocol.client_disconnected();
            return Err(error);
        }
    };
    drop(payload);
    drop(error);
    // There can be no render/fetch in the closing waiter. Drop source/schema,
    // segment and ordinary alias before its first poll. The independently
    // admitted Closing window owns every remaining writer-tail object.
    drop(buffers);
    finish_closing(
        protocol,
        writer,
        capacity,
        Instant::now() + CLOSING_DEADLINE,
    )
    .await
}
async fn finish_closing<'writer, W: AsyncWrite + Unpin>(
    protocol: GovernedProtocolOwner,
    writer: opensrv_mysql::ClosingResponseLease<'writer, W>,
    capacity: novarocks_workload_control::ResultWindowGrant,
    deadline: Instant,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let mut closing = match protocol.into_closing_delivery(writer, capacity, CLOSING_OBJECT_BYTES) {
        Ok(closing) => closing,
        Err((mut protocol, writer, _capacity)) => {
            drop(writer);
            let _ = protocol.client_disconnected();
            return Err(io_error(invalid(
                "Local closing handoff lost its exact owner",
            )));
        }
    };
    match tokio::time::timeout_at(deadline, closing.writer_mut().finish()).await {
        Ok(Ok(writer)) => {
            let _ = closing.settle_after_writer_exit().await;
            writer.no_more_results().await?;
            Ok(MysqlStatementWriteOutcome::Terminated)
        }
        Ok(Err(error)) => {
            drop(closing);
            Err(error)
        }
        Err(_) => {
            drop(closing);
            Err(timeout_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::{
        api::build_string_query_result,
        client_connection::ClientConnectionToken,
        query_control::QueryApplicationControl,
        session_control::{QueryControlService, QuerySessionLease, SessionIdentity},
    };
    use novarocks_workload_control::{
        ResultCapacityConfig, ResultWindowClass, WorkClass, WorkloadConfig, WorkloadControl,
    };
    use opensrv_mysql::{
        AsyncMysqlIntermediary, AsyncMysqlShim, CapabilityFlags, ParamParser, StatementMetaWriter,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    struct Fixture {
        host: WorkloadControl,
        capacity: novarocks_workload_control::ResultCapacityHandle,
        control: QueryControlService,
        session: QuerySessionLease,
    }
    impl Fixture {
        fn new() -> Self {
            let host = WorkloadControl::try_new_counted(WorkloadConfig::default())
                .unwrap()
                .owner;
            let capacity = host
                .configure_result_capacity(ResultCapacityConfig::V1)
                .unwrap();
            host.mark_ready().unwrap();
            let control = QueryControlService::new(Arc::new(QueryApplicationControl::default()));
            let session = control
                .register_session(SessionIdentity::new(
                    ClientConnectionToken::new(901, 1).unwrap(),
                    "root",
                ))
                .unwrap();
            Self {
                host,
                capacity,
                control,
                session,
            }
        }
        fn statement(
            &self,
        ) -> novarocks_query_application::session_control::GovernedQueryStatementOwner {
            self.control
                .begin_governed_statement_with_result(
                    self.session.token(),
                    &self.host.root_admission(),
                    WorkClass::Management,
                    None,
                    None,
                    None,
                    ResultWindowClass::Local,
                )
                .unwrap()
        }
        fn protocol(&self) -> GovernedProtocolOwner {
            GovernedProtocolOwner::new(self.statement())
        }
        fn completion(
            &self,
            affected_rows: u64,
        ) -> novarocks_query_application::protocol_delivery::GovernedCompletionStatementResult
        {
            use novarocks_query_application::{
                protocol_delivery::QuerySessionOutput,
                session_control::{
                    StatementActionReady, StatementActionValue, StatementCommandCompletion,
                },
            };
            let mut ready = Some(StatementActionReady {
                result: Ok(StatementActionValue::Ok(StatementCommandCompletion::new(
                    affected_rows,
                ))),
                owner: self.statement(),
            });
            let mut output = None;
            StatementActionReady::transfer_protocol_in(&mut ready, &mut output).unwrap();
            let Some(QuerySessionOutput::GovernedCompletion(result)) = output else {
                panic!("the original completion retains its statement protocol owner")
            };
            result
        }
        fn result(&self, value: String) -> GovernedImmediateStatementResult {
            let statement = self
                .control
                .begin_governed_statement_with_result(
                    self.session.token(),
                    &self.host.root_admission(),
                    WorkClass::Management,
                    None,
                    None,
                    None,
                    ResultWindowClass::Local,
                )
                .unwrap();
            // The source is called only after its complete producer admission.
            let graph = build_string_query_result("value", vec![value]).unwrap();
            GovernedImmediateStatementResult::try_new(
                graph,
                statement,
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
            )
            .unwrap_or_else(|(error, _)| panic!("{error}"))
        }
    }
    struct Shim {
        fixture: Fixture,
        gate: Arc<WriteGate>,
    }
    struct WriteGate {
        remaining: std::sync::atomic::AtomicUsize,
        pause_flush: std::sync::atomic::AtomicBool,
        flushes_before_pause: std::sync::atomic::AtomicUsize,
        waiter: std::sync::Mutex<Option<std::task::Waker>>,
        blocked: tokio::sync::Notify,
    }
    impl WriteGate {
        fn new() -> Self {
            Self {
                remaining: std::sync::atomic::AtomicUsize::new(usize::MAX),
                pause_flush: std::sync::atomic::AtomicBool::new(false),
                flushes_before_pause: std::sync::atomic::AtomicUsize::new(0),
                waiter: std::sync::Mutex::new(None),
                blocked: tokio::sync::Notify::new(),
            }
        }
        fn release(&self) {
            self.pause_flush
                .store(false, std::sync::atomic::Ordering::SeqCst);
            self.remaining
                .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
            if let Some(waker) = self.waiter.lock().unwrap().take() {
                waker.wake();
            }
        }
    }
    struct GatedWrite<W> {
        inner: W,
        gate: Arc<WriteGate>,
    }
    impl<W: AsyncWrite + Unpin> AsyncWrite for GatedWrite<W> {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            let remaining = self
                .gate
                .remaining
                .load(std::sync::atomic::Ordering::SeqCst);
            if remaining == 0 {
                *self.gate.waiter.lock().unwrap() = Some(cx.waker().clone());
                self.gate.blocked.notify_one();
                return std::task::Poll::Pending;
            }
            let count = bytes.len().min(remaining);
            let result = std::pin::Pin::new(&mut self.inner).poll_write(cx, &bytes[..count]);
            if let std::task::Poll::Ready(Ok(written)) = &result
                && remaining != usize::MAX
            {
                self.gate
                    .remaining
                    .fetch_sub(*written, std::sync::atomic::Ordering::SeqCst);
            }
            result
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            let countdown = self
                .gate
                .flushes_before_pause
                .load(std::sync::atomic::Ordering::SeqCst);
            if countdown != 0
                && self
                    .gate
                    .flushes_before_pause
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
                    == 1
            {
                self.gate
                    .pause_flush
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            if self
                .gate
                .pause_flush
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                *self.gate.waiter.lock().unwrap() = Some(cx.waker().clone());
                self.gate.blocked.notify_one();
                return std::task::Poll::Pending;
            }
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
    fn canonical_fixture_error() -> novarocks_query_application::session_error::QueryServiceError {
        use novarocks_execution_contract::{
            SafeDetail,
            failure::{FailureClass, FailureFact, FailureReason},
        };
        use novarocks_query_application::api::{ExecutionFailureInput, ExecutionFailureSite};
        use std::num::NonZeroU16;
        novarocks_query_application::session_error::QueryServiceError::from_execution_failure(
            ExecutionFailureInput::new(
                FailureFact::new(
                    FailureClass::Resource,
                    FailureReason::new(
                        NonZeroU16::new(u16::MAX).unwrap(),
                        NonZeroU16::new(7).unwrap(),
                    ),
                ),
                SafeDetail::new("selected bounded resource refusal").unwrap(),
                ExecutionFailureSite::Coordinator,
            ),
        )
    }
    #[async_trait::async_trait]
    impl<W: AsyncWrite + Unpin + Send> AsyncMysqlShim<W> for Shim {
        type Error = io::Error;
        async fn on_prepare<'a>(
            &'a mut self,
            _: &'a str,
            _: StatementMetaWriter<'a, W>,
        ) -> io::Result<()> {
            Err(io::Error::other("test does not prepare"))
        }
        async fn on_execute<'a>(
            &'a mut self,
            _: u32,
            _: ParamParser<'a>,
            _: QueryResultWriter<'a, W>,
        ) -> io::Result<()> {
            Err(io::Error::other(
                "test does not execute prepared statements",
            ))
        }
        async fn on_close<'a>(&'a mut self, _: u32)
        where
            W: 'async_trait,
        {
        }
        async fn on_init<'a>(
            &'a mut self,
            schema: &'a str,
            writer: opensrv_mysql::InitWriter<'a, W>,
        ) -> io::Result<()> {
            if schema == "canonical-error" {
                return crate::terminal::write_governed_init_error(
                    canonical_fixture_error(),
                    self.fixture.protocol(),
                    writer,
                )
                .await;
            }
            if schema == "slow-init" {
                self.gate
                    .remaining
                    .store(2, std::sync::atomic::Ordering::SeqCst);
            }
            if schema == "missing" {
                crate::terminal::write_governed_init_error(
                    novarocks_query_application::session_error::QueryServiceError::new(
                        novarocks_query_application::session_error::QueryServiceErrorKind::BadDatabase,
                        "unknown database"), self.fixture.protocol(), writer).await
            } else {
                crate::terminal::write_governed_init_ok(
                    novarocks_query_application::session_control::StatementCommandCompletion::new(
                        0,
                    ),
                    self.fixture.protocol(),
                    writer,
                )
                .await
            }
        }
        async fn on_query<'a>(
            &'a mut self,
            query: &'a str,
            results: QueryResultWriter<'a, W>,
        ) -> io::Result<()> {
            if let Some(rows) = query.strip_prefix("terminal-affected:") {
                let rows = rows.parse::<u64>().map_err(io::Error::other)?;
                let (completion, protocol) = self.fixture.completion(rows).into_parts();
                return crate::terminal::write_governed_terminal_ok(completion, protocol, results)
                    .await;
            }
            if matches!(query, "relay-wait" | "relay-query-timeout" | "relay-budget") {
                use crate::governed_result_writer::streaming_result_tests::{
                    fixture_with_admission_deadline, string_fields, text_body,
                };
                let seconds = if query == "relay-query-timeout" {
                    2
                } else {
                    120
                };
                let mut fixture = fixture_with_admission_deadline(
                    string_fields(),
                    Some(Instant::now() + std::time::Duration::from_secs(seconds)),
                    Some(seconds * 1_000),
                );
                if query == "relay-budget" {
                    self.gate
                        .remaining
                        .store(0, std::sync::atomic::Ordering::SeqCst);
                }
                let result = fixture.result.take().unwrap();
                let produce = async {
                    if query != "relay-budget" {
                        tokio::time::sleep(std::time::Duration::from_secs(35)).await;
                    }
                    let receipt = fixture
                        .producer
                        .enqueue_client_body(0, text_body("1"), 1)
                        .await
                        .unwrap();
                    receipt.wait().await;
                    let receipt = fixture.producer.enqueue_end(1).await;
                    receipt.wait().await;
                    std::future::pending::<()>().await;
                };
                let written = tokio::select! {
                    biased;
                    written = crate::governed_result_writer::write_streaming_query_result_one(result, results) => written,
                    _ = produce => unreachable!("producer remains live until writer exit"),
                };
                assert_eq!(
                    fixture.protocol_capacity.snapshot().held_positions,
                    [0; 4],
                    "the actual response/Closing writer must exit before its capacity is released"
                );
                fixture.producer.finish();
                return match written? {
                    MysqlStatementWriteOutcome::Continue(writer) => writer.no_more_results().await,
                    MysqlStatementWriteOutcome::Terminated => Ok(()),
                };
            }
            if query == "local-budget" {
                self.gate
                    .remaining
                    .store(0, std::sync::atomic::Ordering::SeqCst);
                self.gate
                    .flushes_before_pause
                    .store(2, std::sync::atomic::Ordering::SeqCst);
            }
            if matches!(
                query,
                "terminal"
                    | "terminal-more"
                    | "slow-terminal"
                    | "flush-terminal"
                    | "cancel-terminal"
            ) {
                if query == "slow-terminal" {
                    self.gate
                        .remaining
                        .store(2, std::sync::atomic::Ordering::SeqCst);
                }
                if query == "flush-terminal" {
                    self.gate
                        .pause_flush
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                let more = query == "terminal-more";
                let (completion, protocol) = self
                    .fixture
                    .completion(if more { 7 } else { 0 })
                    .into_parts();
                if query == "cancel-terminal" {
                    self.fixture.control.cancel_session_statement(
                        self.fixture.session.token(),
                        QueryCancellationReason::ExplicitKill {
                            requester_session:
                                novarocks_query_application::session_control::ClientSessionKey::new(
                                    901,
                                )
                                .expect("nonzero fixture requester"),
                        },
                    );
                }
                return match crate::terminal::write_governed_terminal_ok_with_more(
                    completion, protocol, results, more,
                )
                .await?
                {
                    MysqlStatementWriteOutcome::Continue(writer) if more => {
                        match write_local_result_one(
                            self.fixture.result("next".into()),
                            writer,
                            false,
                        )
                        .await?
                        {
                            MysqlStatementWriteOutcome::Continue(writer) => {
                                writer.no_more_results().await
                            }
                            MysqlStatementWriteOutcome::Terminated => Ok(()),
                        }
                    }
                    MysqlStatementWriteOutcome::Continue(writer) => writer.no_more_results().await,
                    MysqlStatementWriteOutcome::Terminated => Ok(()),
                };
            }
            if query == "canonical-error-late-cancel" {
                let error = canonical_fixture_error();
                let protocol = self.fixture.protocol();
                self.fixture.control.cancel_session_statement(
                    self.fixture.session.token(),
                    QueryCancellationReason::ExplicitKill {
                        requester_session:
                            novarocks_query_application::session_control::ClientSessionKey::new(901)
                                .expect("nonzero fixture requester"),
                    },
                );
                return crate::terminal::write_governed_terminal_error(error, protocol, results)
                    .await;
            }
            if matches!(query, "typed-error" | "slow-typed") {
                if query == "slow-typed" {
                    self.gate
                        .remaining
                        .store(2, std::sync::atomic::Ordering::SeqCst);
                }
                let error = novarocks_query_application::session_error::QueryServiceError::new(
                    novarocks_query_application::session_error::QueryServiceErrorKind::BadDatabase,
                    "unknown database",
                );
                return crate::terminal::write_governed_terminal_error(
                    error,
                    self.fixture.protocol(),
                    results,
                )
                .await;
            }
            let value = if matches!(query, "wide" | "partialwide") {
                "x".repeat(128 * 1024)
            } else {
                query.to_owned()
            };
            // For this one-column fixture the count/definition/EOF occupy
            // 5 + 31 + 9 bytes. Stop after two bytes of the row packet header.
            if matches!(query, "partial" | "partialwide") {
                self.gate
                    .remaining
                    .store(47, std::sync::atomic::Ordering::SeqCst);
            }
            let more = query == "multi";
            let results = if query == "cancel-after-ok" {
                self.gate
                    .remaining
                    .store(2, std::sync::atomic::Ordering::SeqCst);
                results
                    .complete_one(opensrv_mysql::OkResponse::default())
                    .await?
            } else {
                results
            };
            let result = self.fixture.result(value);
            if matches!(query, "cancel" | "cancel-after-ok") {
                self.fixture.control.cancel_session_statement(
                    self.fixture.session.token(),
                    QueryCancellationReason::ExplicitKill {
                        requester_session:
                            novarocks_query_application::session_control::ClientSessionKey::new(901)
                                .expect("nonzero fixture requester"),
                    },
                );
            }
            match write_local_result_one(result, results, more).await? {
                MysqlStatementWriteOutcome::Continue(writer) => {
                    if more {
                        match write_local_result_one(
                            self.fixture.result("next".into()),
                            writer,
                            false,
                        )
                        .await?
                        {
                            MysqlStatementWriteOutcome::Continue(writer) => {
                                writer.no_more_results().await
                            }
                            MysqlStatementWriteOutcome::Terminated => Ok(()),
                        }
                    } else {
                        writer.no_more_results().await
                    }
                }
                MysqlStatementWriteOutcome::Terminated => Ok(()),
            }
        }
    }
    async fn read_packet<C: tokio::io::AsyncRead + Unpin>(client: &mut C) -> (u8, Vec<u8>) {
        let mut header = [0; 4];
        client.read_exact(&mut header).await.unwrap();
        let len =
            usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16;
        let mut bytes = vec![0; len];
        client.read_exact(&mut bytes).await.unwrap();
        (header[3], bytes)
    }
    async fn packet<C: AsyncWrite + Unpin>(client: &mut C, sequence: u8, bytes: &[u8]) {
        let size = bytes.len() as u32;
        let mut header = size.to_le_bytes();
        header[3] = sequence;
        client.write_all(&header).await.unwrap();
        client.write_all(bytes).await.unwrap();
    }
    async fn socket() -> (TcpStream, tokio::task::JoinHandle<io::Result<()>>) {
        socket_with(Fixture::new(), Arc::new(WriteGate::new())).await
    }
    async fn socket_with(
        fixture: Fixture,
        gate: Arc<WriteGate>,
    ) -> (TcpStream, tokio::task::JoinHandle<io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, write) = stream.into_split();
            AsyncMysqlIntermediary::run_with_options(
                Shim {
                    fixture,
                    gate: Arc::clone(&gate),
                },
                read,
                GatedWrite { inner: write, gate },
                &crate::MYSQL_INTERMEDIARY_OPTIONS,
            )
            .await
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        handshake(&mut client).await;
        (client, server)
    }
    async fn handshake<C: tokio::io::AsyncRead + AsyncWrite + Unpin>(client: &mut C) {
        let (sequence, greeting) = read_packet(client).await;
        assert_eq!(sequence, 0);
        assert_eq!(greeting[0], 10);
        let capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_PLUGIN_AUTH;
        let mut response = Vec::new();
        response.extend_from_slice(&capabilities.bits().to_le_bytes());
        response.extend_from_slice(&(64_u32 * 1024 * 1024).to_le_bytes());
        response.push(33);
        response.extend_from_slice(&[0; 23]);
        response.extend_from_slice(b"root\0");
        response.push(0);
        response.extend_from_slice(b"mysql_native_password\0");
        packet(client, 1, &response).await;
        assert_eq!(read_packet(client).await.1[0], 0);
    }
    async fn duplex_socket_with(
        fixture: Fixture,
        gate: Arc<WriteGate>,
    ) -> (
        tokio::io::DuplexStream,
        tokio::task::JoinHandle<io::Result<()>>,
    ) {
        let (mut client, stream) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let (read, write) = tokio::io::split(stream);
            AsyncMysqlIntermediary::run_with_options(
                Shim {
                    fixture,
                    gate: Arc::clone(&gate),
                },
                read,
                GatedWrite { inner: write, gate },
                &crate::MYSQL_INTERMEDIARY_OPTIONS,
            )
            .await
        });
        handshake(&mut client).await;
        (client, server)
    }
    async fn wait_for_write_gate(gate: &WriteGate) {
        loop {
            let notified = gate.blocked.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if gate.waiter.lock().unwrap().is_some() {
                return;
            }
            tokio::time::timeout(std::time::Duration::from_secs(300), notified)
                .await
                .expect("actual writer did not reach its controlled gate");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn relay_producer_gap_excludes_active_budget_and_keeps_the_connection() {
        let (mut client, server) =
            duplex_socket_with(Fixture::new(), Arc::new(WriteGate::new())).await;
        let started = Instant::now();
        packet(&mut client, 0, b"\x03relay-wait").await;
        assert_eq!(read_result(&mut client, 1).await.0, b"\x011");
        assert!(started.elapsed() >= std::time::Duration::from_secs(35));
        packet(&mut client, 0, b"\x03again").await;
        assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
        packet(&mut client, 0, &[1]).await;
        server.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn relay_producer_gap_observes_the_original_admission_deadline() {
        let (mut client, server) =
            duplex_socket_with(Fixture::new(), Arc::new(WriteGate::new())).await;
        let started = Instant::now();
        packet(&mut client, 0, b"\x03relay-query-timeout").await;
        for _ in 0..3 {
            read_packet(&mut client).await;
        }
        let (_, error) = read_packet(&mut client).await;
        assert_eq!(&error[..3], &[0xff, 0x25, 0x05]);
        assert!(String::from_utf8_lossy(&error).contains("query timed out after 2000 ms"));
        assert!(started.elapsed() < std::time::Duration::from_secs(30));
        packet(&mut client, 0, b"\x03again").await;
        assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
        packet(&mut client, 0, &[1]).await;
        server.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn relay_metadata_and_data_share_twenty_plus_eleven_budget() {
        let gate = Arc::new(WriteGate::new());
        let (mut client, server) = duplex_socket_with(Fixture::new(), Arc::clone(&gate)).await;
        packet(&mut client, 0, b"\x03relay-budget").await;
        wait_for_write_gate(&gate).await;
        tokio::time::advance(std::time::Duration::from_secs(20)).await;
        // Exactly the three metadata packets; the next actual row write blocks.
        gate.remaining
            .store(45, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = gate.waiter.lock().unwrap().take() {
            waker.wake();
        }
        for _ in 0..3 {
            read_packet(&mut client).await;
        }
        wait_for_write_gate(&gate).await;
        tokio::time::advance(std::time::Duration::from_secs(11)).await;
        let error = server.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        assert!(
            bytes.is_empty(),
            "no row may be published after budget exhaustion"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn local_metadata_and_terminal_flush_share_budget_and_retain_generation() {
        let fixture = Fixture::new();
        let capacity = fixture.capacity.clone();
        let control = fixture.control.clone();
        let session = fixture.session.token();
        let gate = Arc::new(WriteGate::new());
        let (mut client, server) = duplex_socket_with(fixture, Arc::clone(&gate)).await;
        packet(&mut client, 0, b"\x03local-budget").await;
        wait_for_write_gate(&gate).await;
        tokio::time::advance(std::time::Duration::from_secs(20)).await;
        gate.release();
        assert_eq!(read_result(&mut client, 1).await.0, b"\x0clocal-budget");
        wait_for_write_gate(&gate).await;
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert!(
            control.begin_statement(session).is_err(),
            "terminal bytes are not flush exit"
        );
        tokio::time::advance(std::time::Duration::from_secs(11)).await;
        assert_eq!(
            server.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert!(
            control.list_processes().is_empty(),
            "physical writer and connection exit release the session generation"
        );
    }

    async fn read_result<C: tokio::io::AsyncRead + Unpin>(
        client: &mut C,
        first_sequence: u8,
    ) -> (Vec<u8>, Vec<u8>, u8) {
        let count = read_packet(client).await;
        assert_eq!(count, (first_sequence, vec![1]));
        let definition = read_packet(client).await;
        assert_eq!(definition.0, first_sequence.wrapping_add(1));
        let metadata_end = read_packet(client).await;
        assert_eq!(metadata_end.1, [0xfe, 0, 0, 0, 0]);
        let row = read_packet(client).await;
        let end = read_packet(client).await;
        assert_eq!(end.0, first_sequence.wrapping_add(4));
        assert_eq!(end.1[0], 0xfe);
        (row.1, end.1, end.0.wrapping_add(1))
    }
    #[tokio::test]
    async fn local_socket_renders_short_and_cross_turn_rows_and_reenters() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            for (query, expected) in [
                ("hello", b"\x05hello".to_vec()),
                (
                    "wide",
                    [vec![0xfd, 0, 0, 2], vec![b'x'; 128 * 1024]].concat(),
                ),
                ("again", b"\x05again".to_vec()),
            ] {
                packet(&mut client, 0, &[&[3], query.as_bytes()].concat()).await;
                let (row, end, _) = read_result(&mut client, 1).await;
                assert_eq!(row, expected);
                assert_eq!(end, [0xfe, 0, 0, 0, 0]);
            }
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn local_socket_more_results_flag_matches_following_result() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            packet(&mut client, 0, b"\x03multi").await;
            let (row, end, next_sequence) = read_result(&mut client, 1).await;
            assert_eq!(row, b"\x05multi");
            assert_eq!(end, [0xfe, 0, 0, 8, 0]);
            let (row, end, _) = read_result(&mut client, next_sequence).await;
            assert_eq!(row, b"\x04next");
            assert_eq!(end, [0xfe, 0, 0, 0, 0]);
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn initial_local_cancel_writes_error_and_reuses_the_connection() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            packet(&mut client, 0, b"\x03cancel").await;
            let (sequence, error) = read_packet(&mut client).await;
            assert_eq!(sequence, 1);
            assert_eq!(&error[..3], &[0xff, 0x25, 0x05]); // ER_QUERY_INTERRUPTED
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn resident_local_partial_header_moves_only_writer_tail_to_closing() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let session = fixture.session.token();
            let gate = Arc::new(WriteGate::new());
            let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
            packet(&mut client, 0, b"\x03partial").await;
            for _ in 0..3 {
                read_packet(&mut client).await;
            }
            let mut header = [0; 4];
            client.read_exact(&mut header[..2]).await.unwrap();
            control.cancel_session_statement(
                session,
                QueryCancellationReason::ExplicitKill {
                    requester_session:
                        novarocks_query_application::session_control::ClientSessionKey::new(901)
                            .expect("nonzero fixture requester"),
                },
            );
            while capacity.snapshot().held_positions != [0, 0, 0, 1] {
                tokio::task::yield_now().await;
            }
            assert!(
                control.begin_statement(session).is_err(),
                "closing retains the statement generation"
            );
            gate.release();
            client.read_exact(&mut header[2..]).await.unwrap();
            assert_eq!(header, [8, 0, 0, 4]);
            let mut row = [0; 8];
            client.read_exact(&mut row).await.unwrap();
            assert_eq!(&row, b"\x07partial");
            let (sequence, error) = read_packet(&mut client).await;
            assert_eq!(sequence, 5);
            assert_eq!(&error[..3], &[0xff, 0x25, 0x05]);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn incomplete_local_resident_row_disconnects_without_rendering_the_tail() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let session = fixture.session.token();
            let (mut client, server) = socket_with(fixture, Arc::new(WriteGate::new())).await;
            packet(&mut client, 0, b"\x03partialwide").await;
            for _ in 0..3 {
                read_packet(&mut client).await;
            }
            let mut header = [0; 2];
            client.read_exact(&mut header).await.unwrap();
            control.cancel_session_statement(
                session,
                QueryCancellationReason::ExplicitKill {
                    requester_session:
                        novarocks_query_application::session_control::ClientSessionKey::new(901)
                            .expect("nonzero fixture requester"),
                },
            );
            let mut suffix = Vec::new();
            client.read_to_end(&mut suffix).await.unwrap();
            assert!(
                suffix.is_empty(),
                "missing tail cannot be rendered after the cut"
            );
            assert!(server.await.unwrap().is_err());
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn initial_cancel_transfers_before_a_pending_previous_ok_can_block() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let gate = Arc::new(WriteGate::new());
            let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
            packet(&mut client, 0, b"\x03cancel-after-ok").await;
            let mut header = [0; 4];
            client.read_exact(&mut header[..2]).await.unwrap();
            assert_eq!(
                capacity.snapshot().held_positions,
                [0, 0, 0, 1],
                "previous pending OK is already exclusively in Closing"
            );
            gate.release();
            client.read_exact(&mut header[2..]).await.unwrap();
            let len =
                usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16;
            let mut ok = vec![0; len];
            client.read_exact(&mut ok).await.unwrap();
            assert_eq!(header[3], 1);
            assert_eq!(ok[0], 0);
            assert_eq!(ok[3], 8);
            let (sequence, error) = read_packet(&mut client).await;
            assert_eq!(sequence, 2);
            assert_eq!(&error[..3], &[0xff, 0x25, 0x05]);
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn action_completion_count_reaches_the_original_terminal_packet() {
        use mysql_common::{
            io::ParseBuf,
            packets::{CommonOkPacket, OkPacketDeserializer},
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let (mut client, server) = socket_with(fixture, Arc::new(WriteGate::new())).await;
            for rows in [
                0_u64,
                3,
                250,
                251,
                65_535,
                65_536,
                16_777_215,
                16_777_216,
                u64::MAX,
            ] {
                let command = format!("\x03terminal-affected:{rows}");
                packet(&mut client, 0, command.as_bytes()).await;
                let (sequence, payload) = read_packet(&mut client).await;
                assert_eq!(sequence, 1);
                let parsed = ParseBuf(&payload)
                    .parse::<OkPacketDeserializer<CommonOkPacket>>(
                        CapabilityFlags::CLIENT_PROTOCOL_41,
                    )
                    .unwrap()
                    .into_inner();
                assert_eq!(parsed.affected_rows(), rows);
                assert_eq!(parsed.last_insert_id(), None);
                assert!(
                    !parsed
                        .status_flags()
                        .contains(opensrv_mysql::StatusFlags::SERVER_MORE_RESULTS_EXISTS)
                );
            }
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
            assert!(control.list_processes().is_empty());
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn finite_terminal_ok_has_explicit_more_results_and_no_deferred_owner() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            packet(&mut client, 0, b"\x03terminal-more").await;
            assert_eq!(
                read_packet(&mut client).await,
                (1, vec![0, 7, 0, 8, 0, 0, 0])
            );
            assert_eq!(read_result(&mut client, 2).await.0, b"\x04next");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn canonical_init_and_late_cancel_errors_keep_the_selected_wire_descriptor() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let (mut client, server) = socket_with(fixture, Arc::new(WriteGate::new())).await;
            let mut errors = Vec::new();
            for command in [
                b"\x02canonical-error".as_slice(),
                b"\x03canonical-error-late-cancel",
            ] {
                packet(&mut client, 0, command).await;
                errors.push(read_packet(&mut client).await);
            }
            packet(&mut client, 0, b"\x03again").await;
            let mut following = Vec::new();
            for _ in 0..5 {
                following.push(read_packet(&mut client).await);
            }
            packet(&mut client, 0, &[1]).await;
            let exited = server.await.unwrap();
            // The original socket/session owner has exited before any oracle.
            assert!(exited.is_ok());
            for (sequence, error) in errors {
                assert_eq!(sequence, 1);
                assert_eq!(&error[..3], &[0xff, 0x11, 0x04]);
                assert_eq!(&error[4..9], b"HY000");
                assert_eq!(
                    &error[9..],
                    b"[query.execution.resource_exhausted] selected bounded resource refusal"
                );
            }
            assert_eq!(following[3].1, b"\x05again");
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            assert!(control.list_processes().is_empty());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn finite_terminal_and_init_packets_preserve_types_and_next_command() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            for query in [b"\x03terminal".as_slice(), b"\x02db"] {
                packet(&mut client, 0, query).await;
                assert_eq!(
                    read_packet(&mut client).await,
                    (1, vec![0, 0, 0, 0, 0, 0, 0])
                );
            }
            for query in [b"\x03typed-error".as_slice(), b"\x02missing"] {
                packet(&mut client, 0, query).await;
                let (sequence, error) = read_packet(&mut client).await;
                assert_eq!(sequence, 1);
                assert_eq!(&error[..3], &[0xff, 0x19, 0x04]); // ER_BAD_DB_ERROR
                assert_eq!(&error[4..9], b"42000");
                assert_eq!(&error[9..], b"unknown database");
            }
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn slow_terminal_success_retains_ordinary_window_and_generation_until_write_exit() {
        for command in [b"\x03slow-terminal".as_slice(), b"\x02slow-init"] {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let fixture = Fixture::new();
                let capacity = fixture.capacity.clone();
                let control = fixture.control.clone();
                let session = fixture.session.token();
                let gate = Arc::new(WriteGate::new());
                let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
                packet(&mut client, 0, command).await;
                let mut header = [0; 4];
                client.read_exact(&mut header[..2]).await.unwrap();
                assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
                assert!(control.begin_statement(session).is_err());
                gate.release();
                client.read_exact(&mut header[2..]).await.unwrap();
                assert_eq!(header, [7, 0, 0, 1]);
                let mut ok = [0; 7];
                client.read_exact(&mut ok).await.unwrap();
                assert_eq!(ok, [0; 7]);
                packet(&mut client, 0, b"\x03again").await;
                assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
                packet(&mut client, 0, &[1]).await;
                server.await.unwrap().unwrap();
            })
            .await
            .unwrap();
        }
    }
    #[tokio::test]
    async fn slow_typed_error_retires_local_window_but_retains_closing_generation() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let session = fixture.session.token();
            let gate = Arc::new(WriteGate::new());
            let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
            packet(&mut client, 0, b"\x03slow-typed").await;
            let mut header = [0; 4];
            client.read_exact(&mut header[..2]).await.unwrap();
            assert_eq!(capacity.snapshot().held_positions, [0, 0, 0, 1]);
            assert!(control.begin_statement(session).is_err());
            gate.release();
            client.read_exact(&mut header[2..]).await.unwrap();
            let len = usize::from(header[0]);
            let mut error = vec![0; len];
            client.read_exact(&mut error).await.unwrap();
            assert_eq!(&error[..3], &[0xff, 0x19, 0x04]);
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn terminal_flush_holds_ordinary_generation_after_the_complete_packet() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let session = fixture.session.token();
            let gate = Arc::new(WriteGate::new());
            let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
            packet(&mut client, 0, b"\x03flush-terminal").await;
            assert_eq!(read_packet(&mut client).await, (1, vec![0; 7]));
            assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
            assert!(
                control.begin_statement(session).is_err(),
                "packet completion alone is not flush exit"
            );
            gate.release();
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn cancelled_terminal_uses_closing_and_keeps_the_connection() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            packet(&mut client, 0, b"\x03cancel-terminal").await;
            let (sequence, error) = read_packet(&mut client).await;
            assert_eq!(sequence, 1);
            assert_eq!(&error[..3], &[0xff, 0x25, 0x05]);
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn local_buffers_keep_original_window_after_statement_owner_exits() {
        let fixture = Fixture::new();
        let capacity = fixture.capacity.clone();
        let (graph, mut protocol) = fixture.result("hello".into()).into_parts();
        let cursor = graph.into_cursor();
        let buffers = LocalProtocolBuffers {
            _window: cursor.retain_physical_guard(),
            cursor,
            bytes: vec![0; RootProfileV1::SEGMENT_BYTES],
            metadata: None,
        };
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert_eq!(
            protocol.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        protocol.complete();
        // Logical completion cannot free the actual cursor/segment owner.
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(buffers);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
