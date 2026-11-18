// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Ordinary CREATE TABLE retains its admitted generation through publication,
//! one permitted observation, and actual Provider/CPU/I/O exit.

use super::{
    QueryCancellationView, QueryServiceError, governed_cancellation_error,
    product_command_service_error,
};
use crate::connector::mutation::{
    CompletedCatalogMutation, MutationDispatchState, ResolvedCatalogMutation,
    command_error_from_connector, present_catalog_mutation_command_result,
};
use crate::query_execution::{
    internal_result_cpu::InternalResultCpu, maintenance::io_admission::MetadataIoAdmission,
};
use novarocks_query_application::{
    api::{
        CatalogTablePropertyBinding, CommandContext, CommandError, CommandErrorKind,
        StatementExternalEffectDisposition as Effect, StatementNativeHealthFailure,
    },
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue, StatementCommandCompletion,
    },
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReceiving, ConnectorCatalogMutationReconcileRequest,
    ConnectorCatalogMutationRequest, ConnectorError, ConnectorErrorKind, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationOutcome, ProviderMetadataCpuPort,
};
use novarocks_workload_control::{ResultWindowAlias, WorkError};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

type Outcome = ExternalMutationOutcome<ConnectorCatalogMutationReceipt>;
type MutationFuture = Pin<Box<dyn Future<Output = Result<(), ConnectorError>> + Send>>;

#[derive(Debug, Default)]
struct Wake(futures::task::AtomicWaker);
impl crate::wake::StatusIntakeWake for Wake {
    fn wake(&self) {
        self.0.wake();
    }
}
/// A closed payload prepared by the original planning CPU. The type itself
/// grants no CPU authority: the caller must invoke `try_new` in its actual
/// Table property CPU work, then transfer it after that work really exits.
pub(crate) struct PreparedTableMutationInput {
    binding: CatalogTablePropertyBinding,
    original_request: ConnectorCatalogMutationRequest,
    execute_request: ConnectorCatalogMutationRequest,
}
impl PreparedTableMutationInput {
    pub(crate) fn try_new(
        binding: CatalogTablePropertyBinding,
        original_request: ConnectorCatalogMutationRequest,
    ) -> Result<Self, CommandError> {
        let ConnectorCatalogMutationOperation::CreateTable { table, .. } =
            &original_request.operation
        else {
            return Err(CommandError::new(
                CommandErrorKind::Unsupported,
                "table mutation action requires ordinary CREATE TABLE",
            ));
        };
        if table != binding.table()
            || original_request.target.instance_id != binding.mutation().descriptor().instance_id
            || original_request.target.incarnation != binding.planning().binding().incarnation()
        {
            return Err(work_command(WorkError::Conflict));
        }
        if original_request.context.io_admission().is_some()
            || original_request.context.metadata_cpu().is_some()
        {
            return Err(work_command(WorkError::Conflict));
        }
        // SPI execute owns a request while reconciliation borrows the
        // original request. This one necessary body copy belongs to the
        // original planning CPU, never to an Action poll or async body.
        let execute_request = original_request.clone();
        Ok(Self {
            binding,
            original_request,
            execute_request,
        })
    }
}

struct Input {
    binding: CatalogTablePropertyBinding,
    request: ConnectorCatalogMutationRequest,
}

#[derive(Default)]
struct Call {
    future: Option<MutationFuture>,
    receiving: Arc<ConnectorCatalogMutationReceiving>,
    result: Option<Result<(), ConnectorError>>,
    started: bool,
    in_poll: bool,
    dropping: bool,
    exited: bool,
}
impl Call {
    fn start_execute(&mut self, input: Arc<Input>, request: ConnectorCatalogMutationRequest) {
        assert!(!self.started, "one original catalog execute call");
        self.started = true;
        let receiving = self.receiving.clone();
        self.future = Some(Box::pin(async move {
            input
                .binding
                .mutation()
                .execute_async_in(request, &receiving)
                .await
        }));
    }
    fn start_reconcile(&mut self, input: Arc<Input>, evidence: ExternalMutationEvidence) {
        assert!(!self.started, "one original catalog observation call");
        self.started = true;
        let receiving = self.receiving.clone();
        self.future = Some(Box::pin(async move {
            input
                .binding
                .mutation()
                .reconcile_async_in(
                    &input.request,
                    ConnectorCatalogMutationReconcileRequest {
                        evidence,
                        context: input.request.context.clone(),
                    },
                    &receiving,
                )
                .await
        }));
    }
    fn poll(&mut self, cx: &mut Context<'_>) {
        if self.exited || self.in_poll || self.dropping {
            return;
        }
        let Some(future) = self.future.as_mut() else {
            return;
        };
        self.in_poll = true;
        let result = future.as_mut().poll(cx);
        self.in_poll = false;
        if let Poll::Ready(result) = result {
            // SPI kept its raw verdict before Provider Drop. Keep this outer
            // result too, before dropping its original future.
            self.result = Some(result);
            self.dropping = true;
            drop(self.future.take());
            self.dropping = false;
            self.exited = true;
        }
    }
    fn health(&self) -> Option<StatementNativeHealthFailure> {
        (self.in_poll || self.dropping || self.receiving.foreign_exit_unproven()).then(|| {
            StatementNativeHealthFailure::actual_exit_unproven(
                "table mutation original future exit is unproven",
                Arc::new(WorkError::Conflict),
            )
        })
    }
    fn closed(&self) -> bool {
        if !self.started {
            return true;
        }
        let observed = self.receiving.observation();
        // Factory entry determines whether a child exists, not whether a
        // publication happened. It is never an external-effect proof.
        self.exited
            && self.health().is_none()
            && (!observed.dispatched() || observed.provider_exited())
    }
    fn outcome(&self) -> Result<Outcome, ConnectorError> {
        assert!(
            self.closed() && self.started,
            "actual call exit precedes selection"
        );
        let observed = self.receiving.observation();
        match observed.validated_outcome() {
            Some(Ok(outcome)) => Ok(outcome.clone()),
            Some(Err(error)) => Err(error.clone()),
            None => match self.result.as_ref() {
                Some(Err(error)) => Err(error.clone()),
                _ => Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "table mutation exited without an original validated outcome",
                )),
            },
        }
    }
    fn applied(&self) -> bool {
        self.receiving.observation().applied_receipt().is_some()
    }
}

struct Source {
    pending: Option<PreparedTableMutationInput>,
    input: Option<Arc<Input>>,
    command: CommandContext,
    window: ResultWindowAlias,
    cpu: InternalResultCpu,
    cancellation: QueryCancellationView,
    runtime: tokio::runtime::Handle,
    wake: Arc<Wake>,
    io: Option<Arc<MetadataIoAdmission>>,
    metadata_cpu: Option<Arc<dyn ProviderMetadataCpuPort>>,
    setup_attempted: bool,
    bootstrap_ready: bool,
    execute: Call,
    reconcile: Call,
    initial_unknown: Option<Outcome>,
    resolved: Option<ResolvedCatalogMutation>,
    before_call_failure: Option<QueryServiceError>,
    ordinary_failure: Option<QueryServiceError>,
    health: Option<StatementNativeHealthFailure>,
    closed: bool,
}
impl Source {
    fn health(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.execute.health())
            .or_else(|| self.reconcile.health())
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
            .or_else(|| {
                self.metadata_cpu
                    .as_ref()
                    .and_then(|cpu| cpu.local_failure())
                    .map(|cause| {
                        StatementNativeHealthFailure::actual_exit_unproven(
                            "table mutation metadata CPU exit is unproven",
                            cause,
                        )
                    })
            })
    }
    fn effect(&self) -> Option<Effect> {
        if self.execute.applied() || self.reconcile.applied() {
            return Some(Effect::KnownCommitted);
        }
        match self.resolved.as_ref() {
            Some(ResolvedCatalogMutation::KnownCommitted(completed)) => match completed.effect {
                ExternalMutationEffect::Applied => Some(Effect::KnownCommitted),
                ExternalMutationEffect::NoOp => None,
            },
            Some(ResolvedCatalogMutation::KnownUncommitted { .. }) => {
                Some(Effect::KnownUncommitted)
            }
            Some(
                ResolvedCatalogMutation::CommitUnknown { .. }
                | ResolvedCatalogMutation::ContractFailure { .. },
            ) => Some(Effect::CommitUnknown),
            // None means no supplied fact; it is never proof of non-commit.
            None if self.execute.started => Some(Effect::CommitUnknown),
            None => None,
        }
    }
    fn fail_before_call(&mut self, error: QueryServiceError) {
        assert!(!self.execute.started, "do not replace a Provider verdict");
        self.before_call_failure.get_or_insert(error);
    }
    fn setup(&mut self, owner: &mut GovernedActionContext<'_>) {
        if self.setup_attempted || self.before_call_failure.is_some() {
            return;
        }
        self.setup_attempted = true;
        let result = (|| {
            let pending = self.pending.as_ref().expect("one original table input");
            let dependencies = pending
                .binding
                .mutation()
                .initial_dependencies()
                .map_err(command_error_from_connector)?;
            let original = owner
                .execution_owner_slot()
                .as_ref()
                .ok_or_else(|| work_command(WorkError::Conflict))?;
            if !original.is_for_scope(self.command.scope()) {
                return Err(work_command(WorkError::Conflict));
            }
            if let Some(io) = self.io.as_ref() {
                io.check_original_positive_owner(original)
                    .map_err(work_command)?;
                // The original LIKE Source already activated this Root.
                // The Provider extends that admission when routing each
                // actual target request.
                self.bootstrap_ready = true;
            } else {
                self.io = Some(
                    MetadataIoAdmission::for_original_source(original, dependencies)
                        .map_err(work_command)?,
                );
            }
            Ok::<_, CommandError>(())
        })();
        if let Err(error) = result {
            self.fail_before_call(product_command_service_error(error));
        }
    }
    fn start_execute(&mut self) {
        // The actual bootstrap installs the original Root's dependencies.
        // Authenticate the CPU author only after that paid phase is ready.
        let result = (|| {
            let pending = self.pending.as_ref().expect("one original table input");
            let port = self
                .cpu
                .planning_metadata_cpu_port(
                    self.command.scope(),
                    self.window.clone(),
                    pending.binding.planning(),
                    self.cancellation.clone(),
                    self.runtime.clone(),
                    self.wake.clone(),
                )
                .map_err(work_command)?;
            self.metadata_cpu = Some(port.clone());
            let PreparedTableMutationInput {
                binding,
                mut original_request,
                mut execute_request,
            } = self
                .pending
                .take()
                .expect("one original prepared table input");
            // Both requests keep the CPU-prepared bodies. Only the original
            // lightweight context is extended with this Source's exact ports.
            let context = original_request
                .context
                .with_io_admission(self.io.as_ref().unwrap().clone())
                .with_metadata_cpu(port);
            original_request.context = context.clone();
            execute_request.context = context;
            let input = Arc::new(Input {
                binding,
                request: original_request,
            });
            self.input = Some(input.clone());
            self.execute.start_execute(input, execute_request);
            Ok::<_, CommandError>(())
        })();
        if let Err(error) = result {
            self.fail_before_call(product_command_service_error(error));
        }
    }
    fn progress_io(&mut self, owner: &mut GovernedActionContext<'_>, cx: &mut Context<'_>) {
        let Some(io) = self.io.as_ref() else {
            return;
        };
        let result = if self.bootstrap_ready {
            io.poll_actual_progress_continuing(cx)
        } else if self.before_call_failure.is_none() {
            match owner.execution_owner_slot().as_ref() {
                Some(original) => io.poll_progress(original, cx),
                None => Err(WorkError::Conflict),
            }
        } else {
            return;
        };
        match result {
            Ok(ready) => self.bootstrap_ready |= ready,
            Err(error) => {
                self.ordinary_failure
                    .get_or_insert_with(|| work_service(error.clone()));
                if !self.execute.started {
                    self.fail_before_call(work_service(error));
                }
            }
        }
    }
    fn select(&mut self) {
        if self.resolved.is_some() || !self.execute.started || !self.execute.closed() {
            return;
        }
        if self.initial_unknown.is_none() {
            match self.execute.outcome() {
                Ok(unknown @ Outcome::CommitUnknown { .. }) => self.initial_unknown = Some(unknown),
                Ok(outcome) => {
                    self.resolved = Some(resolve(outcome));
                    return;
                }
                Err(error) => {
                    // A raw failure supplies no positive non-publication
                    // evidence. Do not infer it from factory entry or text.
                    self.resolved = Some(ResolvedCatalogMutation::ContractFailure {
                        error,
                        dispatch: MutationDispatchState::PossiblyDispatched,
                    });
                    return;
                }
            }
        }
        if !self.reconcile.started {
            let Some(Outcome::CommitUnknown { evidence, .. }) = self.initial_unknown.as_ref()
            else {
                unreachable!()
            };
            self.reconcile
                .start_reconcile(self.input.as_ref().unwrap().clone(), evidence.clone());
        }
        if !self.reconcile.closed() {
            return;
        }
        // Preserve the original one-observation policy, including its raw
        // failure and evidence when observation is negative or refuses.
        self.resolved = Some(select_recovery(
            self.initial_unknown.as_ref().unwrap(),
            self.reconcile.outcome(),
        ));
    }
    fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.closed {
            return Poll::Ready(());
        }
        if self.health().is_some() || !self.execute.closed() || !self.reconcile.closed() {
            return Poll::Pending;
        }
        if let Some(cpu) = self.metadata_cpu.as_ref() {
            match cpu.poll_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    // A failed close is not an actual-exit receipt, even if
                    // the Provider business result is already available.
                    self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "table mutation metadata CPU close is unproven",
                        Arc::new(error),
                    ));
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
            }
            if self.health().is_some() {
                return Poll::Pending;
            }
        }
        if let Some(io) = self.io.as_ref() {
            let withdrawn = if !self.bootstrap_ready && !self.execute.started {
                match io.withdraw_unstarted_in() {
                    Ok(Some(observed)) => {
                        if let Some(error) = observed.into_ordinary_failure() {
                            self.ordinary_failure
                                .get_or_insert_with(|| work_service(error));
                        }
                        true
                    }
                    Ok(None) => false,
                    Err(error) => {
                        self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                            "table metadata bootstrap withdrawal is unproven",
                            Arc::new(error),
                        ));
                        return Poll::Pending;
                    }
                }
            } else {
                false
            };
            if !withdrawn {
                match io.poll_actual_quiescent_continuing(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(health)) => {
                        self.health = Some(health);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(observed)) => {
                        if let Some(error) = observed.into_ordinary_failure() {
                            self.ordinary_failure
                                .get_or_insert_with(|| work_service(error));
                        }
                    }
                }
            }
        }
        // The I/O owner still retains its independent cleanup diagnostic;
        // do not invent another business verdict from that secondary fact.
        self.closed = true;
        Poll::Ready(())
    }
    fn result(&mut self) -> Result<StatementActionValue, QueryServiceError> {
        assert!(self.closed, "result follows actual quiescence");
        let primary = match self.resolved.clone() {
            Some(resolved) => present_catalog_mutation_command_result(resolved)
                .map(|_| StatementActionValue::Ok(StatementCommandCompletion::empty()))
                .map_err(product_command_service_error),
            None => Err(self
                .before_call_failure
                .take()
                .expect("pre-call refusal or Provider verdict")),
        };
        // Cleanup can fail a successful command without erasing the effect.
        let selected = match primary {
            Ok(value) => self.ordinary_failure.take().map_or(Ok(value), Err),
            error => error,
        };
        // Validated Applied evidence also survives a later contract refusal.
        // Failure class and actual external-effect knowledge stay separate.
        selected.map_err(|error| match self.effect() {
            Some(effect) => error.attach_external_effect_disposition(effect),
            None => error,
        })
    }
}

struct Action {
    source: Option<Box<Source>>,
    delivered: bool,
    effect: Option<Effect>,
}
impl GovernedStatementAction for Action {
    fn external_effect_disposition(&self) -> Option<Effect> {
        self.source
            .as_ref()
            .and_then(|source| source.effect())
            .or(self.effect)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.source.as_ref().and_then(|source| source.health())
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if !self.delivered {
            return Ok(());
        }
        if target.is_some() {
            return Err(StatementNativeHealthFailure::actual_exit_unproven(
                "table mutation residual receiver is occupied",
                Arc::new(WorkError::Conflict),
            ));
        }
        *target = self
            .source
            .take()
            .map(|source| source as Box<dyn GovernedStatementResidual>);
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        assert!(!self.delivered, "one governed table command result");
        let source = self.source.as_mut().expect("same table mutation Source");
        source.wake.0.register(cx.waker());
        if source.health().is_some() {
            return Poll::Pending;
        }
        if !source.execute.started && source.before_call_failure.is_none() {
            if let Some(reason) = owner.cancellation().reason() {
                source.fail_before_call(governed_cancellation_error(reason));
            }
            if !source.window.is_for_scope(owner.scope()) {
                source.fail_before_call(work_service(WorkError::Conflict));
            }
        }
        source.setup(owner);
        source.progress_io(owner, cx);
        if source.bootstrap_ready && source.before_call_failure.is_none() && !source.execute.started
        {
            source.start_execute();
        }
        source.execute.poll(cx);
        source.select();
        source.reconcile.poll(cx);
        source.select();
        // The Provider poll may have published another request to this SAME
        // admission. A borrowed wait cancellation never drops these futures.
        source.progress_io(owner, cx);
        if source.health().is_some() {
            return Poll::Pending;
        }
        if source.resolved.is_none() && source.before_call_failure.is_none() {
            return Poll::Pending;
        }
        std::task::ready!(source.poll_closed(cx));
        self.effect = source.effect();
        self.delivered = true;
        Poll::Ready(source.result())
    }
}
impl GovernedStatementResidual for Source {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = self.health() {
            return Poll::Ready(Err(health));
        }
        self.poll_closed(cx).map(|()| Ok(()))
    }
}
fn select_recovery(
    original: &Outcome,
    recovery: Result<Outcome, ConnectorError>,
) -> ResolvedCatalogMutation {
    match recovery {
        Ok(Outcome::KnownUncommitted { .. }) | Err(_) => resolve(original.clone()),
        Ok(outcome) => resolve(outcome),
    }
}
fn resolve(outcome: Outcome) -> ResolvedCatalogMutation {
    match outcome {
        Outcome::KnownCommitted {
            effect,
            finalization,
            cleanup,
            ..
        } => ResolvedCatalogMutation::KnownCommitted(CompletedCatalogMutation {
            effect,
            finalization,
            cleanup,
        }),
        Outcome::KnownUncommitted { failure, cleanup } => {
            ResolvedCatalogMutation::KnownUncommitted { failure, cleanup }
        }
        Outcome::CommitUnknown { failure, evidence } => {
            ResolvedCatalogMutation::CommitUnknown { failure, evidence }
        }
    }
}
fn work_command(error: WorkError) -> CommandError {
    CommandError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(error))
}
fn work_service(error: WorkError) -> QueryServiceError {
    product_command_service_error(work_command(error))
}

/// The original planning CPU must have constructed this prepared input and
/// actually closed before handoff. This Action only moves its request bodies;
/// it constructs no parser AST, generation, Root or result window.
pub(crate) fn prepare(
    prepared: PreparedTableMutationInput,
    continued_io: Option<Arc<MetadataIoAdmission>>,
    command: CommandContext,
    cpu: InternalResultCpu,
    cancellation: QueryCancellationView,
    runtime: tokio::runtime::Handle,
) -> Result<Box<dyn GovernedStatementAction>, CommandError> {
    prepare_owned(prepared, continued_io, command, cpu, cancellation, runtime)
        .map(|action| Box::new(action) as Box<dyn GovernedStatementAction>)
}

fn prepare_owned(
    prepared: PreparedTableMutationInput,
    continued_io: Option<Arc<MetadataIoAdmission>>,
    command: CommandContext,
    cpu: InternalResultCpu,
    cancellation: QueryCancellationView,
    runtime: tokio::runtime::Handle,
) -> Result<Action, CommandError> {
    let window = command
        .result_window_alias()
        .ok_or_else(|| work_command(WorkError::Conflict))?;
    if !window.is_for_scope(command.scope()) {
        return Err(work_command(WorkError::Conflict));
    }
    Ok(Action {
        source: Some(Box::new(Source {
            pending: Some(prepared),
            input: None,
            command,
            window,
            cpu,
            cancellation,
            runtime,
            wake: Arc::new(Wake::default()),
            io: continued_io,
            metadata_cpu: None,
            setup_attempted: false,
            bootstrap_ready: false,
            execute: Call::default(),
            reconcile: Call::default(),
            initial_unknown: None,
            resolved: None,
            before_call_failure: None,
            ordinary_failure: None,
            health: None,
            closed: false,
        })),
        delivered: false,
        effect: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::*;
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::Waker,
    };

    struct Provider {
        descriptor: ConnectorInstanceDescriptor,
        epoch: ProviderBindingEpoch,
        released: AtomicBool,
        calls: AtomicUsize,
        drops: AtomicUsize,
        original: Arc<u8>,
        raw_error: bool,
        panic_drop: bool,
        effect: ExternalMutationEffect,
    }
    impl Provider {
        fn new(raw_error: bool, panic_drop: bool) -> Arc<Self> {
            Arc::new(Self {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("c").unwrap(),
                },
                epoch: ProviderBindingEpoch::new(),
                released: AtomicBool::new(false),
                calls: AtomicUsize::new(0),
                drops: AtomicUsize::new(0),
                original: Arc::new(7),
                raw_error,
                panic_drop,
                effect: ExternalMutationEffect::Applied,
            })
        }
        fn call(self: &Arc<Self>) -> Call {
            let session = ConnectorCatalogMutationSession::new(
                self.descriptor.clone(),
                ConnectorControlRuntimeId::new(),
                self.epoch,
                self.clone(),
                || {},
            )
            .unwrap();
            let request = ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.epoch,
                },
                operation: ConnectorCatalogMutationOperation::CreateTable {
                    table: ConnectorTableIdentity {
                        instance_id: self.descriptor.instance_id.clone(),
                        namespace: Arc::from("db"),
                        table: Arc::from("t"),
                    },
                    columns: Vec::new(),
                    comment: None,
                    properties: Vec::new(),
                    policy: CreatePolicy::FailIfExists,
                },
                context: ConnectorRequestContext::try_new(
                    std::time::Instant::now() + std::time::Duration::from_secs(30),
                    ConnectorStopOwner::new().view(),
                    1024,
                    4096,
                )
                .unwrap(),
            };
            let mut call = Call {
                started: true,
                ..Call::default()
            };
            let receiving = call.receiving.clone();
            // Exercise the actual SPI original child and the Action's exact
            // Poll/Drop custody. This is not a metadata CPU fixture.
            call.future = Some(Box::pin(async move {
                session.execute_async_in(request, &receiving).await
            }));
            call
        }
    }
    impl ConnectorCatalogMutation for Provider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.epoch
        }
        fn execute(&self, _: ConnectorCatalogMutationRequest) -> Result<Outcome, ConnectorError> {
            panic!("no synchronous fallback")
        }
        fn reconcile(
            &self,
            _: ConnectorCatalogMutationReconcileRequest,
        ) -> Result<Outcome, ConnectorError> {
            panic!("no replay")
        }
        fn execute_async<'a>(
            &'a self,
            request: ConnectorCatalogMutationRequest,
            receiving: &'a ConnectorCatalogMutationReceiving,
        ) -> Pin<Box<dyn Future<Output = Result<Outcome, ConnectorError>> + Send + 'a>> {
            assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
            receiving
                .retain_original_owner(ConnectorProcessLocalPayload::new(
                    request.target.clone(),
                    self.original.clone(),
                ))
                .unwrap();
            Box::pin(Child {
                provider: self,
                request,
                receiving,
            })
        }
    }
    struct Child<'a> {
        provider: &'a Provider,
        request: ConnectorCatalogMutationRequest,
        receiving: &'a ConnectorCatalogMutationReceiving,
    }
    impl Future for Child<'_> {
        type Output = Result<Outcome, ConnectorError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let receipt = ConnectorCatalogMutationReceipt::try_new(
                this.provider.descriptor.clone(),
                this.provider.epoch,
                this.request.operation_id,
                this.request.operation.kind(),
                None,
            )
            .unwrap();
            if !this.provider.raw_error && this.provider.effect == ExternalMutationEffect::Applied {
                this.receiving.record_applied_receipt(&receipt).unwrap();
            }
            if !this.provider.released.load(Ordering::Acquire) {
                return Poll::Pending;
            }
            if this.provider.raw_error {
                return Poll::Ready(Err(ConnectorError::from_domain(
                    ConnectorErrorKind::ResourceExhausted,
                    WorkError::Capacity("original table call refusal"),
                )));
            }
            Poll::Ready(Ok(Outcome::KnownCommitted {
                effect: this.provider.effect,
                receipt,
                finalization: ExternalMutationFinalization::Complete,
                cleanup: ExternalMutationFinalization::Complete,
            }))
        }
    }
    impl Drop for Child<'_> {
        fn drop(&mut self) {
            assert!(self.receiving.observation().raw_outcome().is_some());
            self.provider.drops.fetch_add(1, Ordering::SeqCst);
            assert!(
                !self.provider.panic_drop,
                "actual Provider child destructor failure"
            );
        }
    }
    #[test]
    fn borrowed_wait_does_not_replace_applied_original_or_certify_exit() {
        let provider = Provider::new(false, false);
        let mut call = provider.call();
        let mut cx = Context::from_waker(Waker::noop());
        {
            let mut borrowed = Box::pin(std::future::poll_fn(|cx| {
                call.poll(cx);
                Poll::<()>::Pending
            }));
            assert!(borrowed.as_mut().poll(&mut cx).is_pending());
        }
        assert!(call.applied());
        assert!(!call.closed());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.drops.load(Ordering::SeqCst), 0);
        {
            let observed = call.receiving.observation();
            let actual = observed.original_owner().unwrap().value::<u8>("c").unwrap();
            assert!(Arc::ptr_eq(actual, &provider.original));
            assert!(!observed.provider_exited());
        }
        provider.released.store(true, Ordering::Release);
        call.poll(&mut cx);
        assert!(call.closed());
        assert!(matches!(
            call.outcome(),
            Ok(Outcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                ..
            })
        ));
        assert_eq!(provider.drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn actual_noop_is_not_inferred_from_a_missing_applied_receipt() {
        let mut provider = Provider::new(false, false);
        Arc::get_mut(&mut provider).unwrap().effect = ExternalMutationEffect::NoOp;
        let mut call = provider.call();
        let mut cx = Context::from_waker(Waker::noop());
        call.poll(&mut cx);
        assert!(!call.applied());
        assert!(!call.closed());
        provider.released.store(true, Ordering::Release);
        call.poll(&mut cx);
        assert!(call.closed());
        assert!(matches!(
            call.outcome(),
            Ok(Outcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                ..
            })
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn raw_error_is_not_an_uncommitted_proof_and_keeps_original_typed_cause() {
        let provider = Provider::new(true, false);
        let mut call = provider.call();
        provider.released.store(true, Ordering::Release);
        call.poll(&mut Context::from_waker(Waker::noop()));
        assert!(call.closed());
        assert!(!call.applied());
        let raw = call.outcome().unwrap_err();
        let expected = std::error::Error::source(&raw)
            .unwrap()
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            .unwrap()
            .clone();
        let projected =
            present_catalog_mutation_command_result(ResolvedCatalogMutation::ContractFailure {
                error: raw,
                dispatch: MutationDispatchState::PossiblyDispatched,
            })
            .unwrap_err();
        let custody = std::error::Error::source(projected.failure_cause().unwrap()).unwrap();
        let primary = std::error::Error::source(custody)
            .unwrap()
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            .unwrap();
        assert_eq!(primary, &expected);
        assert_eq!(
            projected.external_effect_disposition(),
            Some(Effect::CommitUnknown)
        );
        assert!(call.receiving.observation().raw_outcome().is_some());
    }
    #[test]
    fn provider_drop_panic_retains_raw_applied_fact_and_refuses_actual_exit() {
        let provider = Provider::new(false, true);
        let mut call = provider.call();
        provider.released.store(true, Ordering::Release);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            call.poll(&mut Context::from_waker(Waker::noop()));
        }));
        assert!(panic.is_err());
        assert!(call.applied());
        assert!(call.health().is_some());
        assert!(!call.closed());
        assert!(call.receiving.foreign_exit_unproven());
        assert!(matches!(
            call.receiving.observation().raw_outcome(),
            Some(Ok(Outcome::KnownCommitted { .. }))
        ));
        call.poll(&mut Context::from_waker(Waker::noop()));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn negative_or_failed_observation_keeps_original_unknown_identity() {
        let provider = Provider::new(false, false);
        let evidence = ExternalMutationEvidence::try_new(
            1,
            provider.descriptor.clone(),
            provider.epoch,
            ConnectorMutationOperationId::new(),
            "create-table",
            bytes::Bytes::from_static(b"original"),
        )
        .unwrap();
        let original_failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "original lost response",
        );
        let original = Outcome::CommitUnknown {
            failure: original_failure.clone(),
            evidence: evidence.clone(),
        };
        let negative = Outcome::KnownUncommitted {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Conflict,
                "not observed",
            ),
            cleanup: ExternalMutationFinalization::Complete,
        };
        for recovery in [
            Ok(negative),
            Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "observation unavailable",
            )),
        ] {
            let ResolvedCatalogMutation::CommitUnknown {
                failure,
                evidence: retained,
            } = select_recovery(&original, recovery)
            else {
                panic!("original unknown must remain");
            };
            assert_eq!(failure, original_failure);
            assert_eq!(retained, evidence);
        }
    }
}

#[cfg(test)]
mod composition_tests;
