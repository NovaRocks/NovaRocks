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

use super::TableMaintenanceEngine;
use std::sync::Arc;

use super::{
    MaintenanceActionState, MaintenanceRequestContext, MaintenanceStatementResult,
    RequestScopedMaintenanceEngine, TableMaintenanceService,
};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;

/// Foreground maintenance command capability.  Each invocation creates a
/// short-lived engine carrying only the Frontend-composed maintenance ports
/// and the already-admitted request execution.  It cannot use the legacy
/// Core application façade or manufacture a topology/cancellation fallback.
#[derive(Clone)]
pub struct MaintenanceCommandExecutor {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
    runtime: tokio::runtime::Handle,
}

impl MaintenanceCommandExecutor {
    pub fn new(
        kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self { kernel, runtime }
    }

    pub(crate) fn prepare_optimize_action(
        &self,
        command: &novarocks_sql::semantic::MaintenanceSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<
        Option<Box<dyn novarocks_query_application::session_control::GovernedStatementAction>>,
        novarocks_table_maintenance::runtime::TerminalError,
    > {
        let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
            command,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )?;
        let crate::table_maintenance::ParsedMaintenanceStatement::SubmitOptimize { name_parts } =
            lowered
        else {
            return Ok(None);
        };
        super::rewrite_submission::prepare(
            &self.kernel,
            &name_parts,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
            execution,
            connector,
        )
        .map(Some)
    }
    pub(crate) fn prepare_direct_rewrite_action(
        &self,
        command: &novarocks_sql::semantic::MaintenanceSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector: &novarocks_spi::connector::ConnectorRequestContext,
        family: novarocks_result_contract::ClientPresentationFamily,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<
        Option<Box<dyn novarocks_query_application::session_control::GovernedStatementAction>>,
        novarocks_table_maintenance::runtime::TerminalError,
    > {
        let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
            command,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )?;
        let crate::table_maintenance::ParsedMaintenanceStatement::Execute { name_parts, action } =
            lowered
        else {
            return Ok(None);
        };
        if !matches!(
            &action,
            crate::table_maintenance::ParsedMaintenanceAction::RewriteDataFiles { .. }
                | crate::table_maintenance::ParsedMaintenanceAction::RewritePositionDeleteFiles { .. }
        ) {
            return Ok(None);
        }
        super::rewrite_job::direct::prepare(
            &self.kernel,
            &name_parts,
            action,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
            execution,
            connector,
            family,
            #[cfg(novarocks_dev_controls)]
            observation,
        )
        .map(Some)
    }

    pub(crate) fn prepare_cleanup_action(
        &self,
        command: &novarocks_sql::semantic::MaintenanceSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector: &novarocks_spi::connector::ConnectorRequestContext,
        family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<
        Option<Box<dyn novarocks_query_application::session_control::GovernedStatementAction>>,
        novarocks_table_maintenance::runtime::TerminalError,
    > {
        let context = MaintenanceRequestContext {
            current_catalog,
            current_database,
        };
        let lowered =
            crate::table_maintenance::lower_semantic_maintenance_statement(command, context)?;
        let crate::table_maintenance::ParsedMaintenanceStatement::Execute {
            name_parts,
            action:
                crate::table_maintenance::ParsedMaintenanceAction::RemoveOrphanFiles { older_than_ms },
        } = lowered
        else {
            return Ok(None);
        };
        let engine = RequestScopedMaintenanceEngine::new(
            self.kernel.clone(),
            execution.clone(),
            connector.clone(),
        );
        let target = engine
            .resolve_target(&name_parts, context)
            .map_err(novarocks_table_maintenance::runtime::TerminalError::pre_dispatch_failed)?;
        super::cleanup_action::prepare(
            &self.kernel,
            target,
            older_than_ms,
            matches!(
                command,
                novarocks_sql::semantic::MaintenanceSqlCommand::Call { .. }
            ),
            execution,
            connector,
            family,
        )
        .map(Some)
    }

    /// Construct the actual original producer, without polling provider I/O.
    /// The caller installs this WHOLE value in its registered Statement Source.
    pub fn prepare_command(
        &self,
        command: &novarocks_sql::semantic::MaintenanceSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<MaintenanceCommandCall, novarocks_table_maintenance::runtime::TerminalError> {
        let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
            command,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )?;
        if !matches!(
            &lowered,
            crate::table_maintenance::ParsedMaintenanceStatement::Execute {
                action: crate::table_maintenance::ParsedMaintenanceAction::RewriteManifests { .. }
                    | crate::table_maintenance::ParsedMaintenanceAction::ExpireSnapshots { .. },
                ..
            }
        ) {
            return Err("command is not owned by the async metadata producer"
                .to_string()
                .into());
        }
        // Pure source selection only: no Product/Provider future is polled.
        let original_engine = RequestScopedMaintenanceEngine::new(
            self.kernel.clone(),
            execution.clone(),
            connector_context.clone(),
        );
        let name_parts = match &lowered {
            crate::table_maintenance::ParsedMaintenanceStatement::Execute {
                name_parts, ..
            } => name_parts,
            _ => unreachable!("closed metadata lower"),
        };
        let target = super::TableMaintenanceEngine::resolve_target(
            &original_engine,
            name_parts,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )?;
        let instance = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|e| e.to_string())?;
        let lease = self
            .kernel
            .connector_control()
            .acquire_current_metadata_maintenance(&instance)
            .map_err(|e| e.to_string())?;
        let profile = lease.io_profile().map_err(|e| e.to_string())?;
        let capacity = execution.result_capacity().ok_or_else(|| {
            novarocks_table_maintenance::runtime::TerminalError::failed(
                "metadata source has no admitted root capacity",
            )
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
        let context = connector_context.clone().with_io_admission(
            io.clone() as Arc<dyn novarocks_spi::connector::ConnectorIoAdmissionPort>
        );
        let engine =
            RequestScopedMaintenanceEngine::new(self.kernel.clone(), execution.clone(), context);
        let original = MaintenanceActionState::default();
        original.install_engine(engine.clone())?;
        let retained = original.clone();
        let service = Arc::clone(self.kernel.service());
        let catalog = current_catalog.map(str::to_owned);
        let database = current_database.to_owned();
        let spark_call = matches!(
            command,
            novarocks_sql::semantic::MaintenanceSqlCommand::Call { .. }
        );
        let future = Box::pin(make_metadata_product(MetadataProductInput {
            service,
            engine,
            retained,
            lowered,
            spark_call,
            catalog,
            database,
        }));
        Ok(MaintenanceCommandCall {
            future: Some(future),
            result: None,
            poll_unproven: false,
            original,
            io: Some(io),
        })
    }

    /// Executes one Query-Application semantic maintenance command.
    ///
    /// The command has already been parser-admitted and lowered, so this
    /// adapter never receives source syntax or performs an AST round trip.
    pub fn execute_non_metadata_command(
        &self,
        command: &novarocks_sql::semantic::MaintenanceSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, novarocks_table_maintenance::runtime::TerminalError> {
        let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
            command,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )?;
        if matches!(
            &lowered,
            crate::table_maintenance::ParsedMaintenanceStatement::SubmitOptimize { .. }
                | crate::table_maintenance::ParsedMaintenanceStatement::Execute {
                    action: crate::table_maintenance::ParsedMaintenanceAction::RewriteManifests { .. }
                        | crate::table_maintenance::ParsedMaintenanceAction::ExpireSnapshots { .. },
                    ..
                }
        ) {
            return Err(
                "governed maintenance command requires its original async statement source"
                    .to_string()
                    .into(),
            );
        }
        let engine = RequestScopedMaintenanceEngine::new(
            self.kernel.clone(),
            execution.clone(),
            connector_context.clone(),
        );
        let spark_call = matches!(
            command,
            novarocks_sql::semantic::MaintenanceSqlCommand::Call { .. }
        );
        let service = self.kernel.service();
        let context = MaintenanceRequestContext {
            current_catalog,
            current_database,
        };
        let state = MaintenanceActionState::default();
        self.runtime
            .block_on(service.handle_typed_statement(&engine, &state, lowered, spark_call, context))
            .map(statement_result)
    }
}

pub(crate) fn is_async_metadata_command(
    command: &novarocks_sql::semantic::MaintenanceSqlCommand,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<bool, novarocks_table_maintenance::runtime::TerminalError> {
    let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
        command,
        MaintenanceRequestContext {
            current_catalog,
            current_database,
        },
    )?;
    Ok(matches!(
        lowered,
        crate::table_maintenance::ParsedMaintenanceStatement::SubmitOptimize { .. }
            | crate::table_maintenance::ParsedMaintenanceStatement::Execute {
                action: crate::table_maintenance::ParsedMaintenanceAction::RewriteManifests { .. }
                    | crate::table_maintenance::ParsedMaintenanceAction::ExpireSnapshots { .. }
                    | crate::table_maintenance::ParsedMaintenanceAction::RewriteDataFiles { .. }
                    | crate::table_maintenance::ParsedMaintenanceAction::RewritePositionDeleteFiles { .. }
                    | crate::table_maintenance::ParsedMaintenanceAction::RemoveOrphanFiles { .. },
                ..
            }
    ))
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

/// Source-owned product future and its independent fixed row. Field order
/// leaves the row alive if completed Product F Drop unwinds. It is not a grant.
pub struct MaintenanceCommandCall {
    future: Option<MaintenanceStatementFuture>,
    result: Option<
        Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    >,
    poll_unproven: bool,
    original: MaintenanceActionState,
    io: Option<Arc<super::io_admission::MetadataIoAdmission>>,
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
        self.io.as_ref().and_then(|io| io.health_failure())
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
        match io.poll_progress(owner, cx) {
            Err(error) => return Poll::Ready(Err(io_terminal_error(error))),
            Ok(false) => return Poll::Pending,
            Ok(true) => {}
        }
        if io.health_failure().is_some() {
            // Only an actual panic/disposition failure is unproven. Ordinary
            // exited abort errors are received and projected below.
            return Poll::Pending;
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

#[derive(Clone)]
pub struct MaintenanceReadCommandExecutor {
    service: Arc<dyn TableMaintenanceService>,
}

impl MaintenanceReadCommandExecutor {
    pub fn new(service: Arc<dyn TableMaintenanceService>) -> Self {
        Self { service }
    }

    /// Executes a semantic `SHOW ALTER TABLE OPTIMIZE` presentation command.
    pub fn execute_command(
        &self,
        statement: &novarocks_sql::semantic::command::ShowOptimizeSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        self.service
            .handle_typed_show_optimize(
                crate::table_maintenance::lower_semantic_show_optimize(statement)?,
                MaintenanceRequestContext {
                    current_catalog,
                    current_database,
                },
            )
            .map(statement_result)
    }
}

fn statement_result(result: MaintenanceStatementResult) -> StatementResult {
    match result {
        MaintenanceStatementResult::Ok => StatementResult::Ok,
        MaintenanceStatementResult::Query(result) => StatementResult::Query(result),
    }
}

#[cfg(test)]
mod tests {
    use super::super::TableMaintenanceEngine;
    use super::*;
    use std::sync::Arc;
    use std::thread;

    struct ReadOnlyService {
        called: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl TableMaintenanceService for ReadOnlyService {
        fn start(&self, _engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
            Ok(())
        }

        fn handle_typed_show_optimize(
            &self,
            _statement: crate::table_maintenance::ParsedShowOptimize,
            _context: MaintenanceRequestContext<'_>,
        ) -> Result<MaintenanceStatementResult, String> {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(MaintenanceStatementResult::Ok)
        }

        async fn shutdown_until(&self, _deadline: std::time::Instant) -> Result<(), String> {
            Ok(())
        }

        fn request_shutdown_for_process_exit(&self) {}
    }

    #[test]
    fn show_optimize_uses_read_only_service_without_engine() {
        let service = Arc::new(ReadOnlyService {
            called: std::sync::atomic::AtomicBool::new(false),
        });
        let executor = MaintenanceReadCommandExecutor::new(Arc::clone(&service) as Arc<_>);
        let statement =
            novarocks_query_application::sql::parse_single_statement("SHOW ALTER TABLE OPTIMIZE")
                .expect("parser statement");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Maintenance(
            novarocks_sql::semantic::MaintenanceSqlCommand::ShowOptimize(command),
        )) = novarocks_query_application::sql::lower_product_sql_command(&statement)
            .expect("lower semantic command")
        else {
            panic!("expected semantic SHOW ALTER TABLE OPTIMIZE");
        };
        let result = executor
            .execute_command(&command, Some("ice"), "db")
            .expect("execute");
        assert!(matches!(result, StatementResult::Ok));
        assert!(service.called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn injected_process_runtime_drives_work_from_a_plain_worker_thread() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let handle = runtime.handle().clone();
        let result = thread::spawn(move || handle.block_on(async { 17_u8 }))
            .join()
            .expect("plain worker must not require a current Tokio reactor");
        assert_eq!(result, 17);
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
            future: Some(Box::pin(OriginalGate {
                release: receiver,
                polls: Arc::clone(&polls),
                drops: Arc::clone(&drops),
            })),
            result: None,
            poll_unproven: false,
            original: state,
            io: None,
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
            future: Some(Box::pin(ReadyThenPanic)),
            result: None,
            poll_unproven: false,
            original: MaintenanceActionState::default(),
            io: None,
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
