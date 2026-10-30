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

//! Original DROP/ALTER control steps. The enclosing command owns Root and Store.
mod requests;
mod source;
#[cfg(test)]
mod tests;

use super::{CommandInput, MvPhaseFailure, MvWake, original_error};
use crate::{
    mv::domain::application::{MvAlterStatement, MvDropStatement},
    query_execution::maintenance::io_admission::MetadataIoAdmission,
    state_store::execution::StateStoreOriginalOwner,
};
use novarocks_mv_application::{
    activity::MvActivityOwner,
    ports::{MvDropProgressPort, MvProviderFailure, MvProviderFailureKind},
    product::{MvOperationContext, MvTarget},
    readiness::{MvDropReadiness, MvKeyedProjectionDeleteGuard},
    service::MvDropTransition,
};
use novarocks_query_application::{
    api::{StatementExternalEffectDisposition, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_control::GovernedActionContext,
    session_error::QueryServiceError,
};
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_sql::planning::mv::SqlMvTarget;
use source::{ControlSource, Phase, PhaseTask};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Clone)]
pub(crate) enum MvControlRequest {
    Drop {
        target: SqlMvTarget,
        statement: MvDropStatement,
    },
    Alter {
        target: SqlMvTarget,
        statement: MvAlterStatement,
    },
}
impl MvControlRequest {
    pub(super) fn target(&self) -> &SqlMvTarget {
        match self {
            Self::Drop { target, .. } | Self::Alter { target, .. } => target,
        }
    }
    pub(super) fn owner(&self) -> MvActivityOwner {
        match self {
            Self::Drop { .. } => MvActivityOwner::Drop,
            Self::Alter { .. } => MvActivityOwner::Alter,
        }
    }
}

pub(super) struct MvControlStepOriginal {
    input: Arc<CommandInput>,
    request: MvControlRequest,
    io: Arc<MetadataIoAdmission>,
    root: novarocks_workload_control::WorkScope,
    window: novarocks_workload_control::ResultWindowAlias,
    requests: Arc<requests::DropProjectionRequests>,
    context: ConnectorRequestContext,
    wake: Arc<MvWake>,
    source: Option<Arc<ControlSource>>,
    phase: Option<PhaseTask>,
    prepared: bool,
    projected: bool,
    transition: Option<MvDropTransition<MvKeyedProjectionDeleteGuard>>,
    selected: Option<Result<(), MvPhaseFailure>>,
    health: Option<StatementNativeHealthFailure>,
    progress_refusal: Option<novarocks_workload_control::WorkError>,
}
impl MvControlStepOriginal {
    pub(super) fn new(
        input: Arc<CommandInput>,
        io: Arc<MetadataIoAdmission>,
        store: &StateStoreOriginalOwner,
        request: MvControlRequest,
        context: ConnectorRequestContext,
        wake: Arc<MvWake>,
    ) -> Result<Self, QueryServiceError> {
        let capacity = io.root_capacity().map_err(original_error)?;
        let root = capacity.root_scope().clone();
        let window = capacity.try_window_alias().map_err(original_error)?;
        let context = context.with_io_admission(io.clone());
        let requests = Arc::new(requests::DropProjectionRequests::new(Arc::new(
            store
                .execution_requests(
                    input.state_store.transaction_deadline,
                    Some(context.deadline()),
                )
                .map_err(QueryServiceError::from_failure_cause)?,
        )));
        Ok(Self {
            input,
            request,
            io,
            root,
            window,
            requests,
            context,
            wake,
            source: None,
            phase: None,
            prepared: false,
            projected: false,
            transition: None,
            selected: None,
            health: None,
            progress_refusal: None,
        })
    }
    pub(super) fn physical_exit_observed(&self) -> bool {
        self.source
            .as_ref()
            .is_none_or(|source| source.physical_exit_observed())
    }
    pub(super) fn performed_publication(&self) -> bool {
        self.source.as_ref().is_some_and(|s| s.applied())
    }
    pub(super) fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.source
            .as_ref()
            .and_then(|s| s.external_effect_disposition())
    }
    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.source.as_ref().and_then(|s| s.local_health_failure()))
    }
    fn select(
        &mut self,
        mut result: Result<(), MvPhaseFailure>,
    ) -> Poll<Result<(), MvPhaseFailure>> {
        if let Some(source) = &self.source {
            if let Err(error) = source.finish_management()
                && result.is_ok()
            {
                result = Err(super::step::product_failure(error.into_product_error()));
            }
            if result.is_ok()
                && let Some(error) = source.finalization_failure()
            {
                result = Err(super::step::product_failure(error.into_product_error()));
            }
            source.release_activity();
        }
        self.selected = Some(result.clone());
        Poll::Ready(result)
    }
    pub(super) fn poll_business(
        &mut self,
        store: &StateStoreOriginalOwner,
        owner: &mut GovernedActionContext,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MvPhaseFailure>> {
        self.progress(store, cx);
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        let result = self.poll_business_inner(store, owner, cx);
        self.progress(store, cx);
        result
    }
    fn progress(&mut self, store: &StateStoreOriginalOwner, cx: &mut Context<'_>) {
        if let Err(error) = self.io.poll_continuing(cx) {
            self.progress_refusal.get_or_insert(error);
        }
        if let Err(cause) = store.poll_progress(cx) {
            self.health.get_or_insert_with(|| {
                StatementNativeHealthFailure::actual_exit_unproven(
                    "MV control StateStore actual progress is unproven",
                    Arc::new(cause),
                )
            });
        }
    }
    fn poll_business_inner(
        &mut self,
        store: &StateStoreOriginalOwner,
        owner: &mut GovernedActionContext,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MvPhaseFailure>> {
        if let Some(result) = &self.selected {
            return Poll::Ready(result.clone());
        }
        if self.source.is_none() {
            if let Some(error) = &self.progress_refusal {
                return self.select(Err(original_error(error.clone()).into()));
            }
            let cancellation =
                QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
            let source = ControlSource::new(
                self.input.clone(),
                self.request.clone(),
                self.context.clone(),
                self.io.clone(),
                self.root.clone(),
                self.window.clone(),
                self.requests.clone(),
                cancellation,
                self.wake.clone(),
            )?;
            self.source = Some(source);
        }
        let source = self.source.as_ref().unwrap().clone();
        if let Some(error) = &self.progress_refusal {
            source.receive_business_refusal(error.clone());
        }
        source.poll_progress(cx);
        if source.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let MvControlRequest::Drop { statement, .. } = &self.request {
            if self.transition.is_none() {
                self.transition = Some(MvDropTransition::new(
                    MvOperationContext {
                        operation_id: source.operation_id(),
                    },
                    source.product_target().clone(),
                    statement.if_exists,
                ));
            }
            let mut port = DropPort {
                source: &source,
                task: &mut self.phase,
                store,
                duration: self.input.state_store.transaction_deadline,
            };
            if self
                .input
                .product
                .poll_drop(cx, self.transition.as_mut().unwrap(), &mut port)
                .is_pending()
            {
                source.poll_progress(cx);
                return Poll::Pending;
            }
            let result = self
                .transition
                .as_ref()
                .unwrap()
                .result()
                .expect("original DROP terminal")
                .as_ref()
                .map(|_| ())
                .map_err(|error| super::step::product_failure(error.clone()));
            return self.select(result);
        }
        if !self.prepared {
            let result =
                std::task::ready!(poll_phase(&source, &mut self.phase, Phase::Prepare, cx));
            if let Err(error) = result {
                return self.select(Err(super::step::product_failure(
                    error.into_product_error(),
                )));
            }
            self.prepared = true;
        }
        let result = std::task::ready!(source.poll_publication(cx));
        if let Err(error) = result {
            return self.select(Err(super::step::product_failure(
                error.into_product_error(),
            )));
        }
        // NoOp has no necessary Current authority and no false committed marker.
        if !source.applied() {
            return self.select(Ok(()));
        }
        if !self.projected {
            if let Err(error) =
                source.select_existing_requests(store, self.input.state_store.transaction_deadline)
            {
                return self.select(Err(super::step::product_failure(
                    error.into_product_error(),
                )));
            }
            let result =
                std::task::ready!(poll_phase(&source, &mut self.phase, Phase::Project, cx));
            if let Err(error) = result {
                return self.select(Err(super::step::product_failure(
                    error.into_product_error(),
                )));
            }
            self.projected = true;
        }
        self.select(Ok(()))
    }
    pub(super) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        let Some(source) = &self.source else {
            return Poll::Ready(Ok(()));
        };
        source.poll_progress(cx);
        if let Some(task) = &mut self.phase {
            if task.poll(cx).is_pending() {
                return Poll::Pending;
            }
            // Receive the original result before retiring this borrowed phase.
            let _ = source.take_response();
            self.phase.take();
        }
        source.poll_closed(cx)
    }
}

fn poll_phase(
    source: &Arc<ControlSource>,
    task: &mut Option<PhaseTask>,
    phase: Phase,
    cx: &mut Context<'_>,
) -> Poll<Result<(), MvProviderFailure>> {
    if task.is_none() {
        *task = Some(source.phase(phase));
    }
    std::task::ready!(task.as_mut().unwrap().poll(cx));
    let result = source.take_response();
    // PhaseTask has already received raw and dropped its real F normally.
    task.take();
    Poll::Ready(result)
}
struct DropPort<'a> {
    source: &'a Arc<ControlSource>,
    task: &'a mut Option<PhaseTask>,
    store: &'a StateStoreOriginalOwner,
    duration: std::time::Duration,
}
impl MvDropProgressPort<MvKeyedProjectionDeleteGuard> for DropPort<'_> {
    fn poll_prepare_drop(
        &mut self,
        cx: &mut Context<'_>,
        _: MvOperationContext,
        _: &MvTarget,
        _: bool,
    ) -> Poll<Result<MvDropReadiness<MvKeyedProjectionDeleteGuard>, MvProviderFailure>> {
        std::task::ready!(poll_phase(self.source, self.task, Phase::Prepare, cx))?;
        Poll::Ready(self.source.take_drop_readiness())
    }
    fn poll_drop_target(
        &mut self,
        cx: &mut Context<'_>,
        _: MvOperationContext,
        _: &MvTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        std::task::ready!(self.source.poll_publication(cx))?;
        if !self.source.applied() {
            return Poll::Ready(Err(MvProviderFailure::new(
                MvProviderFailureKind::KnownUncommitted,
                "MV DROP returned NoOp; the original projection was not deleted",
            )));
        }
        Poll::Ready(
            self.source
                .select_existing_requests(self.store, self.duration),
        )
    }
    fn poll_delete_after_provider_drop(
        &mut self,
        cx: &mut Context<'_>,
        _: MvOperationContext,
        guard: &mut Option<MvKeyedProjectionDeleteGuard>,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if self.task.is_none() {
            self.source.receive_delete_guard(guard)?;
        }
        poll_phase(self.source, self.task, Phase::DeleteProjection, cx)
    }
    fn poll_unregister_target(
        &mut self,
        cx: &mut Context<'_>,
        _: MvOperationContext,
        _: &MvTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        poll_phase(self.source, self.task, Phase::Unregister, cx)
    }
}
