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

//! Closed Statistics preparation in the original InternalResultCpu factory.
use super::*;
mod scoped_mv_read;
pub(crate) mod scoped_mv_refresh;
use crate::query_execution::statistics::preparation::{
    StatisticsPlanCpuInput, StatisticsPreparationCustody, StatisticsPreparationFailure,
    StatisticsPreparationOutcome,
};

// One actual Box pointee in the original retirement packet. No open closure,
// Any input or caller-selected factory enters the finite worker.
#[expect(
    clippy::large_enum_variant,
    reason = "The original retirement already owns this closed input in one Box; keeping variants inline preserves its same-box loan without a second allocation."
)]
pub(super) enum ClosedCreatorInput {
    Read(super::super::provider_read_facts::ProviderReadCreatorInput),
    MvRefresh(scoped_mv_refresh::MvRefreshCpuTurn),
    MvRead(Arc<crate::query::mv_read_action::MvReadOriginal>),
    ViewShow(Arc<crate::query::view_read_action::ViewShowOriginal>),
    StatisticsPlan(StatisticsPlanCpuInput),
    StatisticsProvider(novarocks_spi::connector::StatisticsCpuInput),
    ProviderMetadata(novarocks_spi::connector::ProviderMetadataCpuInput),
    RewriteProvider(novarocks_spi::connector::RewriteCpuInput),
}
impl ClosedCreatorInput {
    pub(super) fn run_once(&mut self) {
        match self {
            Self::Read(input) => input.run_once(),
            Self::MvRefresh(input) => input.run_once(),
            Self::MvRead(input) => input.run_once(),
            Self::ViewShow(input) => input.run_once(),
            Self::StatisticsPlan(input) => input.run_once(),
            Self::StatisticsProvider(input) => input.run_original_phase(),
            Self::ProviderMetadata(input) => input.run_original_phase(),
            Self::RewriteProvider(input) => input.run_original_phase(),
        }
    }
    pub(super) fn take_read_result_in(
        &mut self,
        target: &mut Option<
            Result<
                novarocks_sql::compiler::ProviderReadFact,
                super::super::provider_read_facts::ProviderReadError,
            >,
        >,
    ) -> Result<(), &'static str> {
        match self {
            Self::Read(input) => input.take_result_in(target),
            _ => Err("closed creator input is not a read creator"),
        }
    }
}

type StatisticsReadCpuPorts = (
    Arc<dyn StatisticsCpuPort>,
    Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
);

impl InternalResultCpu {
    /// Reuse the existing unqualified creator fixture only for linear custody
    /// and actual-exit tests; production always requires its bound source.
    #[cfg(test)]
    pub(crate) fn submit_statistics_plan_fixture(
        &self,
        input: StatisticsPlanCpuInput,
        window: ResultWindowAlias,
        cancellation: QueryCancellationView,
        runtime: &tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> StatisticsPlanCpuJob {
        StatisticsPlanCpuJob {
            original: self.submit_creator_input(
                ClosedCreatorInput::StatisticsPlan(input),
                window,
                None,
                None,
                cancellation,
                runtime,
                wake,
            ),
        }
    }
    pub(crate) fn submit_statistics_plan(
        &self,
        input: StatisticsPlanCpuInput,
        window: ResultWindowAlias,
        source: InternalCpuActionSource,
        cancellation: QueryCancellationView,
        runtime: &tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> StatisticsPlanCpuJob {
        StatisticsPlanCpuJob {
            original: self.submit_creator_input(
                ClosedCreatorInput::StatisticsPlan(input),
                window,
                None,
                Some(source),
                cancellation,
                runtime,
                wake,
            ),
        }
    }
}

/// Only a typed receiving facade; the same original job owns all receipts,
/// forwarder, physical claim and cause. A cancelled waiter drops only a borrow.
pub(crate) struct StatisticsPlanCpuJob {
    original: ProviderReadCpuJob,
}
impl StatisticsPlanCpuJob {
    pub(crate) fn poll_finish_in(
        &mut self,
        cx: &mut std::task::Context<'_>,
        destination: &mut Option<StatisticsPreparationOutcome>,
        custody: &mut StatisticsPreparationCustody,
    ) -> std::task::Poll<Result<(), Arc<dyn std::error::Error + Send + Sync>>> {
        use std::task::Poll;
        let original = &mut self.original;
        let job = original
            .job
            .as_mut()
            .expect("same original statistics CPU job");
        if original.marker.is_none() {
            original.marker = Some(std::task::ready!(job.poll_take(cx)));
        }
        // Neither the result marker nor a published outcome is actual exit.
        std::task::ready!(job.poll_forwarder_closed(cx))?;
        if destination.is_none() {
            match original.marker.as_ref() {
                Some(Ok(value))
                    if matches!(
                        value.value(),
                        Ok(InternalDomainSlot::ProviderCreatorFinished)
                    ) =>
                {
                    let mut loan = ProviderCreatorInputLoan::take(&job.retirement);
                    let Some(ClosedCreatorInput::StatisticsPlan(input)) = loan.input.as_deref_mut()
                    else {
                        panic!("same statistics input");
                    };
                    input
                        .take_result_in(destination)
                        .expect("one statistics result receiver");
                    drop(loan);
                }
                Some(Err(_)) => {
                    let Some(Err(error)) = original.marker.take() else {
                        unreachable!()
                    };
                    *destination = Some(Err(StatisticsPreparationFailure::Cpu(Box::new(error))));
                }
                _ => panic!("closed statistics creator returned a foreign marker"),
            }
        }
        Self::return_access(original, custody);
        original.retire_sources()?;
        Poll::Ready(Ok(()))
    }
    fn return_access(
        original: &mut ProviderReadCpuJob,
        custody: &mut StatisticsPreparationCustody,
    ) {
        let job = original
            .job
            .as_mut()
            .expect("same original statistics CPU job");
        let mut loan = ProviderCreatorInputLoan::take(&job.retirement);
        // A completed successful poll may already have retired its input.
        if let Some(ClosedCreatorInput::StatisticsPlan(input)) = loan.input.as_deref_mut() {
            input.return_access_in(custody);
        }
    }
    pub(crate) async fn close(
        &mut self,
        custody: &mut StatisticsPreparationCustody,
    ) -> Result<(), Arc<dyn std::error::Error + Send + Sync>> {
        let original = &mut self.original;
        let job = original
            .job
            .as_mut()
            .expect("same original statistics CPU job");
        if !job.closing {
            job.closing = true;
            if let Err(cause) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.completion.close()))
            {
                job.retirement
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .provider_close_failure = Some(QueryBlockingFailure::panicked(cause));
            }
        }
        std::future::poll_fn(|cx| job.poll_forwarder_closed(cx)).await?;
        // The same receiving slot survives borrowed cancellation and retains
        // every unused access until the real Provider has closed.
        Self::return_access(original, custody);
        original.retire_sources()
    }
}

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorStatisticsLease, StatisticsCpuInput,
    StatisticsCpuPort,
};
use std::{
    num::NonZeroU64,
    task::{Context, Poll},
};

struct CreatorPortState {
    job: Option<ProviderReadCpuJob>,
    busy: bool,
}
#[derive(Clone)]
pub(super) enum CreatorCpuMode {
    InternalProvider,
    ForegroundBinding,
    ForegroundRead,
    ProviderMetadata,
    ProviderMetadataSettlement(
        Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
    ),
    RewriteBound {
        provider: std::any::TypeId,
        write_begin: std::any::TypeId,
    },
}
pub(super) struct ScopedProviderCreatorCpu {
    cpu: InternalResultCpu,
    scope: WorkScope,
    window: ResultWindowAlias,
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    incarnation: novarocks_spi::connector::ProviderBindingEpoch,
    source_type: std::any::TypeId,
    mode: CreatorCpuMode,
    control: CpuWorkControl,
    runtime: tokio::runtime::Handle,
    wake: Arc<dyn StatusIntakeWake>,
    state: std::sync::Mutex<CreatorPortState>,
}
struct CreatorPortLoan<'a> {
    source: &'a ScopedProviderCreatorCpu,
    job: Option<ProviderReadCpuJob>,
}
impl<'a> CreatorPortLoan<'a> {
    fn take(source: &'a ScopedProviderCreatorCpu) -> Result<Self, ConnectorError> {
        let mut state = source.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.busy {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Statistics CPU source already has its borrowed observer",
            ));
        }
        state.busy = true;
        Ok(Self {
            source,
            job: state.job.take(),
        })
    }
}
impl Drop for CreatorPortLoan<'_> {
    fn drop(&mut self) {
        let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            state.job.is_none() && state.busy,
            "same provider creator CPU slot"
        );
        state.job = self.job.take();
        state.busy = false;
    }
}
const CPU_REFUSAL_DETAIL: &str = "Statistics CPU original action refused";
#[derive(Debug)]
struct StatisticsCpuRefusal(novarocks_failure_contract::FailureFact);
impl std::fmt::Display for StatisticsCpuRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(CPU_REFUSAL_DETAIL)
    }
}
impl std::error::Error for StatisticsCpuRefusal {}
impl novarocks_failure_contract::FailureProjection for StatisticsCpuRefusal {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.0
    }
}
fn original_cpu_refusal(error: &ExecutionFailureInput) -> ConnectorError {
    ConnectorError::from_domain_static(
        statistics_failure_kind(error.fact()),
        StatisticsCpuRefusal(error.fact()),
        CPU_REFUSAL_DETAIL,
    )
    .map_message(|_| error.detail().as_str().to_owned())
}

fn statistics_failure_kind(fact: novarocks_failure_contract::FailureFact) -> ConnectorErrorKind {
    use novarocks_failure_contract::FailureClass;
    match fact.class() {
        FailureClass::Resource => ConnectorErrorKind::ResourceExhausted,
        FailureClass::Control => ConnectorErrorKind::Interrupted,
        FailureClass::Data | FailureClass::Representation => ConnectorErrorKind::CorruptData,
        FailureClass::Dependency => ConnectorErrorKind::Unavailable,
        FailureClass::Unsupported => ConnectorErrorKind::Unsupported,
        FailureClass::Invariant | FailureClass::Unexpected => ConnectorErrorKind::Internal,
    }
}
fn port_unproven() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "original Provider creator CPU cause remains in its source",
    )
}
impl InternalResultCpu {
    /// Original private Source constructor. The provider input cannot mint
    /// this capability: the same bound factory and current full-D node must
    /// authenticate before any input is accepted or worker call published.
    pub(crate) fn statistics_cpu_port(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        lease: &ConnectorStatisticsLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<Arc<dyn StatisticsCpuPort>, WorkError> {
        if !window.is_for_scope(scope) || window.class() != ResultWindowClass::Internal {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        self.make_statistics_cpu_port(
            scope,
            window,
            lease,
            cancellation,
            runtime,
            wake,
            CreatorCpuMode::InternalProvider,
        )
    }
    /// Foreground Statistics metadata capture only. Existing Local/Client
    /// retention is authentic; no Internal decoder/collection capacity is minted.
    pub(crate) fn statistics_binding_cpu_port(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        lease: &ConnectorStatisticsLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<Arc<dyn StatisticsCpuPort>, WorkError> {
        if !window.is_for_scope(scope)
            || !matches!(
                window.class(),
                ResultWindowClass::Local | ResultWindowClass::Client
            )
        {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        self.make_statistics_cpu_port(
            scope,
            window,
            lease,
            cancellation,
            runtime,
            wake,
            CreatorCpuMode::ForegroundBinding,
        )
    }
    fn make_statistics_cpu_port(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        lease: &ConnectorStatisticsLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
        mode: CreatorCpuMode,
    ) -> Result<Arc<dyn StatisticsCpuPort>, WorkError> {
        // Pure authentication, no paid ticket or physical reservation.
        let _ = self.provider_creator_source(scope)?;
        let source_type = lease.scoped_cpu_source_type_id().map_err(|_| {
            WorkError::InvalidConfig("original Statistics CPU factory is unsupported")
        })?;
        Ok(Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor: lease.descriptor().clone(),
            incarnation: lease.incarnation(),
            source_type,
            mode,
            control: cancellation.into(),
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        }))
    }
    pub(crate) fn statistics_read_cpu_ports(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        lease: &ConnectorStatisticsLease,
        planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<StatisticsReadCpuPorts, WorkError> {
        if !window.is_for_scope(scope)
            || !matches!(
                window.class(),
                ResultWindowClass::Local | ResultWindowClass::Client
            )
        {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        let _ = self.provider_creator_source(scope)?;
        let (key, source_type) = planning
            .binding()
            .metadata()
            .provider_metadata_cpu_author()
            .ok_or(WorkError::InvalidConfig(
                "selected metadata CPU author is unsupported",
            ))?;
        if key.instance_id != lease.descriptor().instance_id
            || key.incarnation != lease.incarnation()
            || lease
                .scoped_cpu_source_type_id()
                .map_err(|_| WorkError::Conflict)?
                != source_type
        {
            return Err(WorkError::Conflict);
        }
        let source = Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor: lease.descriptor().clone(),
            incarnation: lease.incarnation(),
            source_type,
            mode: CreatorCpuMode::ForegroundRead,
            control: cancellation.into(),
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        });
        Ok((source.clone(), source))
    }
    pub(crate) fn statistics_port_metadata_bytes_for_composition() -> Result<NonZeroU64, WorkError>
    {
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        let bytes = arc_layout(std::alloc::Layout::new::<ScopedProviderCreatorCpu>())
            .map_err(|_| WorkError::ArithmeticOverflow)?
            .size()
            .checked_add(if cfg!(target_os = "macos") { 64 } else { 0 })
            // Marker remains live while map_message forms the new bounded
            // copy. Its old static message and original cause detail also
            // coexist until replacement; no payload graph is copied.
            .and_then(|n| {
                n.checked_add(
                    metadata_geometry::geometry()
                        .ok()?
                        .action_detail_backing_bytes(),
                )
            })
            .and_then(|n| n.checked_add(CPU_ACTION_DETAIL_BYTES))
            .and_then(|n| {
                n.checked_add(
                    ConnectorError::static_domain_backing_bytes::<StatisticsCpuRefusal>(
                        CPU_REFUSAL_DETAIL,
                    )
                    .ok()?,
                )
            })
            .ok_or(WorkError::ArithmeticOverflow)?;
        NonZeroU64::new(u64::try_from(bytes).map_err(|_| WorkError::ArithmeticOverflow)?)
            .ok_or(WorkError::ArithmeticOverflow)
    }
}
impl ScopedProviderCreatorCpu {
    fn poll_original(
        &self,
        cx: &mut Context<'_>,
        closing: bool,
    ) -> Poll<Result<(), ConnectorError>> {
        let mut loan = match CreatorPortLoan::take(self) {
            Ok(loan) => loan,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let Some(original) = loan.job.as_mut() else {
            return if closing {
                Poll::Ready(Ok(()))
            } else {
                Poll::Ready(Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Statistics CPU source has no original job",
                )))
            };
        };
        if closing {
            let job = original.job.as_mut().expect("same original statistics job");
            if !job.closing {
                job.closing = true;
                if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    job.completion.close()
                })) {
                    job.retirement
                        .state
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .provider_close_failure = Some(QueryBlockingFailure::panicked(cause));
                }
            }
        } else if original.marker.is_none() {
            let job = original.job.as_mut().unwrap();
            original.marker = Some(std::task::ready!(job.poll_take(cx)));
        }
        let job = original.job.as_mut().expect("same original statistics job");
        if std::task::ready!(job.poll_forwarder_closed(cx)).is_err() {
            return Poll::Ready(Err(port_unproven()));
        }
        let projected = match original.marker.as_ref() {
            Some(Err(error)) if !closing => Some(original_cpu_refusal(error)),
            _ => None,
        };
        // Same named original source destruction algorithm, lock-free, only
        // after real physical exit+forwarder join. Any raw failure stays there.
        if original.retire_sources().is_err() {
            return Poll::Ready(Err(port_unproven()));
        }
        let complete = loan.job.take();
        drop(loan); // restore stable empty slot before original header Drop
        drop(complete);
        Poll::Ready(projected.map_or(Ok(()), Err))
    }
}
impl StatisticsCpuPort for ScopedProviderCreatorCpu {
    fn submit_in(&self, input: &mut Option<StatisticsCpuInput>) -> Result<(), ConnectorError> {
        let source = input.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Statistics CPU input is empty",
            )
        })?;
        if matches!(
            self.mode,
            CreatorCpuMode::RewriteBound { .. }
                | CreatorCpuMode::ProviderMetadata
                | CreatorCpuMode::ProviderMetadataSettlement(_)
        ) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Statistics input cannot use a Rewrite-only CPU source",
            ));
        }
        if matches!(self.mode, CreatorCpuMode::ForegroundBinding)
            && !matches!(source, StatisticsCpuInput::Binding(_))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "foreground Statistics CPU source supports metadata binding only",
            ));
        }
        if matches!(self.mode, CreatorCpuMode::ForegroundRead)
            && !matches!(source, StatisticsCpuInput::Read(_))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "foreground read CPU cannot dispatch a Statistics collection/binding phase",
            ));
        }
        if source.original_source_type_id() != self.source_type
            || source.descriptor() != &self.descriptor
            || source.incarnation() != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Statistics CPU input belongs to a foreign original Provider generation",
            ));
        }
        let mut loan = CreatorPortLoan::take(self)?;
        if loan.job.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Statistics CPU source retains its previous original job",
            ));
        }
        let source = self.cpu.provider_creator_source(&self.scope).map_err(|e| {
            ConnectorError::from_domain(statistics_failure_kind(e.failure_fact()), e)
        })?;
        let window = self.window.try_clone().map_err(|e| {
            ConnectorError::from_domain(statistics_failure_kind(e.failure_fact()), e)
        })?;
        loan.job = Some(self.cpu.submit_creator_input_with_control(
            ClosedCreatorInput::StatisticsProvider(input.take().expect("checked input")),
            window,
            None,
            Some(source),
            self.control.clone(),
            &self.runtime,
            Arc::clone(&self.wake),
        ));
        Ok(())
    }
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_original(cx, false)
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_original(cx, true)
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .job
            .as_ref()
            .and_then(ProviderReadCpuJob::health_failure)
    }
    fn metadata_bytes(&self) -> Result<NonZeroU64, ConnectorError> {
        InternalResultCpu::statistics_port_metadata_bytes_for_composition()
            .map_err(|e| ConnectorError::from_domain(ConnectorErrorKind::ResourceExhausted, e))
    }
}

// Only component fixtures can bypass the production lease constructor. They
// still use the same positive bound scope, factory, physical offer and job.
#[cfg(test)]
pub(crate) struct StatisticsCpuFixture {
    original: Arc<ScopedProviderCreatorCpu>,
}
#[cfg(test)]
impl InternalResultCpu {
    pub(crate) fn statistics_cpu_fixture(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
        source_type: std::any::TypeId,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<(Arc<dyn StatisticsCpuPort>, StatisticsCpuFixture), WorkError> {
        if !window.is_for_scope(scope) || window.class() != ResultWindowClass::Internal {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        let _ = self.provider_creator_source(scope)?;
        let original = Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor,
            incarnation,
            source_type,
            mode: CreatorCpuMode::InternalProvider,
            control: cancellation.into(),
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        });
        Ok((original.clone(), StatisticsCpuFixture { original }))
    }
}
#[cfg(test)]
impl StatisticsCpuFixture {
    pub(crate) fn worker_failure_is<T: std::any::Any>(&self) -> bool {
        self.original
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .job
            .as_ref()
            .is_some_and(|job| job.fixture_worker_failure_is::<T>())
    }
    pub(crate) fn take_joined_for_disposition(
        &self,
        observed: &novarocks_query_application::api::StatementNativeHealthFailure,
    ) -> Result<ProviderCreatorFixtureDisposition, &'static str> {
        let mut loan = CreatorPortLoan::take(&self.original)
            .map_err(|_| "fixture requires the original free observer slot")?;
        loan.job
            .as_mut()
            .ok_or("fixture requires the original job")?
            .take_joined_for_fixture_disposition(observed)
    }
}

mod scoped_rewrite;

impl novarocks_spi::connector::ProviderMetadataCpuPort for ScopedProviderCreatorCpu {
    fn submit_metadata_in(
        &self,
        input: &mut Option<novarocks_spi::connector::ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        let original = input.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata CPU input is empty",
            )
        })?;
        if !matches!(
            self.mode,
            CreatorCpuMode::ForegroundRead
                | CreatorCpuMode::ProviderMetadata
                | CreatorCpuMode::ProviderMetadataSettlement(_)
                | CreatorCpuMode::RewriteBound { .. }
        ) || original.original_source_type_id() != self.source_type
            || original.descriptor() != &self.descriptor
            || original.incarnation() != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata CPU input differs from its selected original author",
            ));
        }
        let mut loan = CreatorPortLoan::take(self)?;
        if loan.job.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata source retains its serial CPU job",
            ));
        }
        let source = match &self.mode {
            CreatorCpuMode::ProviderMetadataSettlement(io) => {
                self.cpu.metadata_cpu_continuation_source(io, &self.scope)
            }
            _ => self.cpu.provider_creator_source(&self.scope),
        }
        .map_err(|e| ConnectorError::from_domain(statistics_failure_kind(e.failure_fact()), e))?;
        let window = self.window.try_clone().map_err(|e| {
            ConnectorError::from_domain(statistics_failure_kind(e.failure_fact()), e)
        })?;
        loan.job = Some(self.cpu.submit_creator_input_with_control(
            ClosedCreatorInput::ProviderMetadata(input.take().unwrap()),
            window,
            None,
            Some(source),
            self.control.clone(),
            &self.runtime,
            self.wake.clone(),
        ));
        Ok(())
    }
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_original(cx, false)
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_original(cx, true)
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .job
            .as_ref()
            .and_then(ProviderReadCpuJob::health_failure)
    }
}

#[cfg(test)]
mod refusal_tests {
    use super::*;

    #[test]
    fn original_cpu_marker_keeps_safe_detail_and_neutral_class() {
        for (work, kind) in [
            (
                WorkError::Capacity("fixed proposal and reply cells"),
                ConnectorErrorKind::ResourceExhausted,
            ),
            (WorkError::Closed, ConnectorErrorKind::Interrupted),
            (WorkError::Conflict, ConnectorErrorKind::Internal),
        ] {
            let marker = ExecutionFailureInput::new(
                work.failure_fact(),
                cpu_action_failure_detail(CpuActionFailureSite::Begin, &work),
                ExecutionFailureSite::Coordinator,
            );
            let error = original_cpu_refusal(&marker);
            assert_eq!(error.kind(), kind);
            assert_eq!(error.failure_fact(), marker.fact());
            assert_eq!(error.message(), marker.detail().as_str());
            assert!(error.message().contains("Begin"));
            assert!(std::error::Error::source(&error).is_some());
        }
    }
}
