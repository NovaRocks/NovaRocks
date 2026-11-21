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

//! Closed table-maintenance command capabilities.

use std::sync::Arc;

use super::{
    MaintenanceActionState, MaintenanceRequestContext, MaintenanceStatementResult,
    RequestScopedMaintenanceEngine, TableMaintenanceService,
};
use novarocks_query_application::api::CommandError;
use novarocks_table_maintenance::runtime::TerminalError;

mod table_execute;
pub(crate) use table_execute::{PreparedTableExecute, PreparedTableProcedureMutation};

/// Foreground maintenance command capability.  Each invocation creates a
/// short-lived engine carrying only the Frontend-composed maintenance ports
/// and the already-admitted request execution.  It cannot use the legacy
/// Core application façade or manufacture a topology/cancellation fallback.
#[derive(Clone)]
pub struct MaintenanceCommandExecutor {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
}

impl MaintenanceCommandExecutor {
    pub fn new(kernel: crate::query_execution::kernels::MaintenanceExecutionKernel) -> Self {
        Self { kernel }
    }

    fn prepare_metadata_source(
        &self,
        lowered: crate::table_maintenance::ParsedMaintenanceStatement,
        spark_call: bool,
        context: MaintenanceRequestContext<'_>,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        lease: novarocks_spi::connector::ConnectorMetadataMaintenanceSession,
        retained: Option<table_execute::MetadataPreparation>,
    ) -> Result<MaintenanceCommandCall, CommandError> {
        let profile = lease.io_profile().map_err(|e| e.to_string())?;
        let capacity = execution.result_capacity().ok_or_else(|| {
            crate::query::maintenance_command_error(TerminalError::failed(
                "metadata source has no admitted root capacity",
            ))
        })?;
        // Frontend fixed Source/header/F inventory is already paid by the
        // original per-root process backing. Do not charge it again here.
        let provider_bytes = profile.geometry().metadata_bytes().get();
        let io = super::io_admission::MetadataIoAdmission::new(
            capacity.root_scope().clone(),
            profile,
            provider_bytes,
        )
        .map_err(|e| format!("metadata source construction refused: {e:?}"))?;
        let connector = connector_context.clone().with_io_admission(
            io.clone() as Arc<dyn novarocks_spi::connector::ConnectorIoAdmissionPort>
        );
        let needs_metadata_cpu = retained.as_ref().is_some_and(|retained| matches!(
            retained.intent,
            crate::connector::metadata_maintenance::MetadataMaintenanceIntent::TrinoExpireSnapshots {
                inputs: novarocks_spi::connector::ConnectorExpireSnapshotsExecute {
                    clean_expired_metadata: true,
                    ..
                },
                ..
            }
        ));
        let pending = MetadataProductPreparation {
            kernel: self.kernel.clone(),
            execution: execution.clone(),
            connector,
            lease,
            retained,
            lowered,
            spark_call,
            catalog: context.current_catalog.map(str::to_owned),
            database: context.current_database.to_owned(),
        };
        let original = MaintenanceActionState::default();
        let (pending, future) = if needs_metadata_cpu {
            // Preparing the I/O Source left its original Root dormant.
            // Keep the exact inputs until the Action installs its dependencies.
            (Some(Box::new(pending)), None)
        } else {
            (
                None,
                Some(
                    pending
                        .start(&original, None)
                        .map_err(crate::query::maintenance_command_error)?,
                ),
            )
        };
        Ok(MaintenanceCommandCall {
            pending,
            future,
            result: None,
            poll_unproven: false,
            original,
            io: Some(io),
            metadata_cpu: None,
        })
    }
}

fn maintenance_target_error(
    error: crate::catalog_application::resolver::TargetResolutionError,
) -> CommandError {
    crate::query::maintenance_command_error(TerminalError::from(error))
}

type MaintenanceStatementFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    MaintenanceStatementResult,
                    novarocks_table_maintenance::runtime::TerminalError,
                >,
            > + Send,
    >,
>;

#[derive(Default)]
struct MetadataCpuWake(futures::task::AtomicWaker);

impl std::fmt::Debug for MetadataCpuWake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MetadataCpuWake")
    }
}

impl crate::wake::StatusIntakeWake for MetadataCpuWake {
    fn wake(&self) {
        self.0.wake();
    }
}

struct MetadataCpu {
    port: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    wake: Arc<MetadataCpuWake>,
    close_failure: Option<novarocks_query_application::api::StatementNativeHealthFailure>,
}

impl MetadataCpu {
    fn local_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.close_failure.clone().or_else(|| {
            self.port.local_failure().map(|cause| {
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "snapshot expiration metadata CPU exit is unproven",
                cause,
            )
        })
        })
    }
}

struct MetadataProductPreparation {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
    execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
    connector: novarocks_spi::connector::ConnectorRequestContext,
    lease: novarocks_spi::connector::ConnectorMetadataMaintenanceSession,
    retained: Option<table_execute::MetadataPreparation>,
    lowered: crate::table_maintenance::ParsedMaintenanceStatement,
    spark_call: bool,
    catalog: Option<String>,
    database: String,
}

impl MetadataProductPreparation {
    fn bind_metadata_cpu(
        &self,
        io: &super::io_admission::MetadataIoAdmission,
    ) -> Result<MetadataCpu, novarocks_workload_control::WorkError> {
        let retained = self
            .retained
            .as_ref()
            .expect("clean expiration has the original table binding");
        let runtime = self.kernel.rewrite_runtime()?;
        let capacity = io.root_capacity()?;
        let wake = Arc::new(MetadataCpuWake::default());
        let port = runtime
            .execution
            .0
            .provider_creator_cpu()?
            .planning_metadata_cpu_port(
                capacity.root_scope(),
                capacity.try_window_alias()?,
                &retained.planning,
                self.execution.cancellation().clone(),
                runtime.execution.0.runtime().clone(),
                wake.clone(),
            )?;
        Ok(MetadataCpu {
            port,
            wake,
            close_failure: None,
        })
    }

    fn start(
        self,
        original: &MaintenanceActionState,
        metadata_cpu: Option<&MetadataCpu>,
    ) -> Result<MaintenanceStatementFuture, TerminalError> {
        let connector = match metadata_cpu {
            Some(cpu) => self.connector.with_metadata_cpu(cpu.port.clone()),
            None => self.connector,
        };
        let mut engine = RequestScopedMaintenanceEngine::new(
            self.kernel.clone(),
            self.execution,
            connector.clone(),
        );
        if let Some(retained) = self.retained {
            let table = RequestScopedMaintenanceEngine::target_identity(&retained.target)
                .map_err(TerminalError::from)?;
            let call = crate::connector::metadata_maintenance::MetadataMaintenanceCall::try_new_on_session(
                self.lease,
                novarocks_spi::connector::ConnectorMutationOperationId::new(),
                table,
                retained.intent,
                connector,
            )
            .map_err(|error| TerminalError::from(format!("{error:?}")))?;
            original.install_call(call).map_err(TerminalError::from)?;
            engine = engine.with_metadata_target(retained.target, retained.planning);
        }
        original
            .install_engine(engine.clone())
            .map_err(TerminalError::from)?;
        Ok(Box::pin(make_metadata_product(MetadataProductInput {
            service: Arc::clone(self.kernel.service()),
            engine,
            retained: original.clone(),
            lowered: self.lowered,
            spark_call: self.spark_call,
            catalog: self.catalog,
            database: self.database,
        })))
    }
}

/// Source-owned product future and its independent fixed row. Field order
/// leaves the row alive if completed Product F Drop unwinds. It is not a grant.
pub struct MaintenanceCommandCall {
    pending: Option<Box<MetadataProductPreparation>>,
    future: Option<MaintenanceStatementFuture>,
    result: Option<
        Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    >,
    poll_unproven: bool,
    original: MaintenanceActionState,
    io: Option<Arc<super::io_admission::MetadataIoAdmission>>,
    metadata_cpu: Option<MetadataCpu>,
}
/// Only exact remaining object custody crosses the business completion cut.
/// The Product F, origin/result window and execution context stay out of it.
struct MetadataCleanupResidual {
    call: Option<crate::connector::metadata_maintenance::MetadataMaintenanceCall>,
    io: Option<Arc<super::io_admission::MetadataIoAdmission>>,
}
impl novarocks_query_application::session_control::GovernedStatementResidual
    for MetadataCleanupResidual
{
    fn poll_retirement(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), novarocks_query_application::api::StatementNativeHealthFailure>>
    {
        use novarocks_spi::connector::ConnectorMetadataCleanupResponsibility as R;
        if self
            .call
            .as_ref()
            .is_some_and(|call| call.cleanup_responsibility() == R::Retained)
        {
            // A recorded cleanup failure is not actual disposal. Keep the same
            // report/lease/charge, without retrying publication or a completed F.
            return std::task::Poll::Pending;
        }
        // A panic here leaves this original residual Box and its other field
        // in the QA restoration loan. In particular io outlives Call Drop.
        drop(self.call.take());
        drop(self.io.take());
        std::task::Poll::Ready(Ok(()))
    }
}
impl MaintenanceCommandCall {
    pub(crate) fn has_applied_publication(&self) -> Result<bool, String> {
        self.original.has_applied_publication()
    }

    pub(crate) fn handoff_residual_in(
        &mut self,
        target: &mut Option<
            Box<dyn novarocks_query_application::session_control::GovernedStatementResidual>,
        >,
    ) -> Result<(), novarocks_query_application::api::StatementNativeHealthFailure> {
        let failure = |message: String| {
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "metadata residual transfer is unproven",
                Arc::new(novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    message,
                )),
            )
        };
        if target.is_some() {
            return Err(failure("metadata residual receiver is occupied".into()));
        }
        if self.future.is_some() || self.poll_unproven || self.result.is_some() {
            return Err(failure(
                "metadata business source has not actually completed".into(),
            ));
        }
        if !self.original.needs_cleanup_custody().map_err(failure)? {
            return Ok(());
        }
        let mut residual = Box::new(MetadataCleanupResidual {
            call: None,
            io: None,
        });
        self.original
            .take_cleanup_call_in(&mut residual.call)
            .map_err(failure)?;
        residual.io = self.io.take();
        // Stable recipient precedes foreign execution/result-window cleanup.
        *target = Some(residual);
        self.original.drop_execution_aliases().map_err(failure)?;
        Ok(())
    }

    pub fn publication_disposition(
        &self,
    ) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
        self.original.publication_disposition()
    }

    pub fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.io
            .as_ref()
            .and_then(|io| io.health_failure())
            .or_else(|| {
                self.metadata_cpu
                    .as_ref()
                    .and_then(MetadataCpu::local_failure)
            })
    }

    /// Only the original registered Source borrows its stable WorkOwner.
    /// A cancelled UI never owns this sole owner, Product F, or request table.
    pub fn poll_result(
        &mut self,
        owner: &novarocks_workload_control::WorkOwner,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    > {
        use std::task::Poll;
        let io = match self.io.as_ref() {
            Some(io) => Arc::clone(io),
            None => {
                return Poll::Ready(Err(
                    novarocks_table_maintenance::runtime::TerminalError::failed(
                        "metadata producer has no original I/O admission source",
                    ),
                ));
            }
        };
        if let Some(cpu) = &self.metadata_cpu {
            cpu.wake.0.register(cx.waker());
        }
        match io.poll_progress(owner, cx) {
            Err(_) if self.metadata_cpu.is_some() => {
                // Give the same Product and CPU phase their real exit even
                // when ordinary I/O control has already failed.
                let _ = io.poll_actual_progress_continuing(cx);
            }
            Err(error) => return Poll::Ready(Err(io_terminal_error(error))),
            Ok(false) => return Poll::Pending,
            Ok(true) => {}
        }
        if self.local_health_failure().is_some() {
            // Only an actual panic/disposition failure is unproven. Ordinary
            // exited abort errors are received and projected below.
            return Poll::Pending;
        }
        if let Some(pending) = self.pending.take() {
            let started = pending
                .bind_metadata_cpu(&io)
                .map_err(TerminalError::from_original_cause)
                .and_then(|cpu| {
                    cpu.wake.0.register(cx.waker());
                    self.metadata_cpu = Some(cpu);
                    pending.start(&self.original, self.metadata_cpu.as_ref())
                });
            match started {
                Ok(future) => self.future = Some(future),
                // No Provider call has been polled. The same source still
                // settles its paid I/O, plus the CPU port if it was bound.
                Err(error) => self.result = Some(Err(error)),
            }
        }
        if self.result.is_none() {
            match self.poll_product(cx) {
                Poll::Pending => {
                    let _ = io.poll_progress(owner, cx);
                    return Poll::Pending;
                }
                Poll::Ready(result) => self.result = Some(result),
            }
        }
        if let Some(cpu) = &mut self.metadata_cpu {
            match cpu.port.poll_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    cpu.close_failure = Some(
                        novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                            "snapshot expiration metadata CPU close is unproven",
                            Arc::new(error),
                        ),
                    );
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        // A Product result is not proof that its original response/stream or
        // in-flight credential acquisition has exited. Keep its actual result
        // stable and drive the same finite cells to their real last alias cut.
        let quiescent = io.poll_quiescent(owner, cx);
        // This turn may itself have received an ordinary failure whose later
        // cause Drop panicked. Do not publish success before that health cut.
        if io.health_failure().is_some() {
            return Poll::Pending;
        }
        match quiescent {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                if matches!(self.result, Some(Err(_))) {
                    self.project_saved_cleanup(io.cleanup_failure());
                    Poll::Ready(
                        self.result
                            .take()
                            .expect("original Product primary retained"),
                    )
                } else {
                    Poll::Ready(Err(io_terminal_error(error)))
                }
            }
            Poll::Ready(Ok(())) => {
                self.project_saved_cleanup(io.cleanup_failure());
                Poll::Ready(self.result.take().expect("original saved Product result"))
            }
        }
    }

    /// Both primary Product result and exact publication snapshot stay in the
    /// original Call while the one received cleanup failure is projected.
    fn project_saved_cleanup(&mut self, cleanup: Option<novarocks_spi::connector::ConnectorError>) {
        let Some(cleanup) = cleanup else {
            return;
        };
        let result = self
            .result
            .as_mut()
            .expect("Product result stable before cleanup projection");
        if let Err(primary) = result {
            // Do not replace state/class/control/cause with a cleanup failure.
            primary.message.push_str(" (cleanup: ");
            primary.message.push_str(cleanup.message());
            primary.message.push(')');
        }
        // An exited writer abort failure is cleanup evidence, not proof that a
        // required business finalization failed. The Product alone classifies
        // its actual publication and necessary finalization. Keep its success;
        // the same Source retains the typed cleanup receipt and original cause.
    }

    fn poll_product(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    > {
        use std::task::Poll;
        assert!(
            !self.poll_unproven,
            "unproven maintenance producer cannot be repolled"
        );
        self.poll_unproven = true;
        match self
            .future
            .as_mut()
            .expect("producer consumed once")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => {
                self.poll_unproven = false;
                Poll::Pending
            }
            Poll::Ready(result) => {
                self.result = Some(result);
                drop(self.future.take());
                self.poll_unproven = false;
                Poll::Ready(self.result.take().expect("saved before F Drop"))
            }
        }
    }
}

#[cfg(test)]
mod original_producer_tests {
    use super::*;
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };

    struct OriginalGate {
        release: futures::channel::oneshot::Receiver<()>,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Future for OriginalGate {
        type Output =
            Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Pin::new(&mut self.release).poll(cx).map(|result| {
                result.expect("original gate sender");
                Ok(MaintenanceStatementResult::Ok)
            })
        }
    }
    impl Drop for OriginalGate {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn cancelled_borrow_keeps_the_same_product_future_and_original_row() {
        let (release, receiver) = futures::channel::oneshot::channel();
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let state = MaintenanceActionState::default();
        let mut original = MaintenanceCommandCall {
            pending: None,
            future: Some(Box::pin(OriginalGate {
                release: receiver,
                polls: Arc::clone(&polls),
                drops: Arc::clone(&drops),
            })),
            result: None,
            poll_unproven: false,
            original: state,
            io: None,
            metadata_cpu: None,
        };
        use futures::FutureExt;
        let mut wait = Box::pin(std::future::poll_fn(|cx| original.poll_product(cx)));
        assert!(wait.as_mut().now_or_never().is_none());
        drop(wait);
        assert!(original.future.is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        assert!(matches!(
            std::future::poll_fn(|cx| original.poll_product(cx)).await,
            Ok(MaintenanceStatementResult::Ok)
        ));
        assert!(original.future.is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(polls.load(Ordering::SeqCst) >= 2);
    }
    struct ReadyThenPanic;
    impl Future for ReadyThenPanic {
        type Output =
            Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(MaintenanceStatementResult::Ok))
        }
    }
    impl Drop for ReadyThenPanic {
        fn drop(&mut self) {
            std::panic::panic_any(73_u32)
        }
    }
    #[test]
    fn completed_product_drop_panic_keeps_ready_and_original_row_outside_future() {
        let mut original = MaintenanceCommandCall {
            pending: None,
            future: Some(Box::pin(ReadyThenPanic)),
            result: None,
            poll_unproven: false,
            original: MaintenanceActionState::default(),
            io: None,
            metadata_cpu: None,
        };
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original.poll_product(&mut cx)
        })) else {
            panic!("actual completed F Drop must panic")
        };
        assert_eq!(cause.downcast_ref::<u32>(), Some(&73));
        assert!(matches!(
            original.result,
            Some(Ok(MaintenanceStatementResult::Ok))
        ));
        assert!(original.future.is_none() && original.poll_unproven);
        // Same row remains independently reachable after the actual F Drop.
        assert!(original.original.has_engine().is_ok());
    }
}

fn io_terminal_error(
    error: novarocks_workload_control::WorkError,
) -> novarocks_table_maintenance::runtime::TerminalError {
    use novarocks_failure_contract::FailureProjection;
    novarocks_table_maintenance::runtime::TerminalError::from(format!(
        "metadata I/O admission refused: {error:?}"
    ))
    .with_execution_failure(Some(
        novarocks_query_application::execution_error::ExecutionFailureInput::new(
            error.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating("metadata I/O admission refused"),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator,
        ),
    ))
}

struct MetadataProductInput {
    service: Arc<dyn TableMaintenanceService>,
    engine: RequestScopedMaintenanceEngine,
    retained: MaintenanceActionState,
    lowered: crate::table_maintenance::ParsedMaintenanceStatement,
    spark_call: bool,
    catalog: Option<String>,
    database: String,
}
async fn make_metadata_product(
    input: MetadataProductInput,
) -> Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError> {
    input
        .service
        .handle_typed_statement(
            &input.engine,
            &input.retained,
            input.lowered,
            input.spark_call,
            MaintenanceRequestContext {
                current_catalog: input.catalog.as_deref(),
                current_database: &input.database,
            },
        )
        .await
}
#[cfg(test)]
#[path = "command/cleanup_reception_tests.rs"]
mod cleanup_reception_tests;
