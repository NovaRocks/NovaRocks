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

//! Keep the original property CPU and its actual exit through command handoff.

use crate::query_execution::completion_facts::FrontendPlanningCpuSource;
use novarocks_query_application::{
    api::StatementNativeHealthFailure,
    session_control::{GovernedStatementAction, GovernedStatementResidual},
    session_error::QueryServiceError,
};
use novarocks_workload_control::WorkError;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

type Preparation<T> = Pin<
    Box<
        dyn Future<Output = Result<Result<T, QueryServiceError>, StatementNativeHealthFailure>>
            + Send,
    >,
>;
pub(super) struct PropertyActionTail<T> {
    pub(super) cpu: Arc<FrontendPlanningCpuSource>,
    pub(super) preparation: Option<Preparation<T>>,
    pub(super) prepared: Option<Result<T, QueryServiceError>>,
    pub(super) inner: Option<Box<dyn GovernedStatementAction>>,
    pub(super) inner_residual: Option<Box<dyn GovernedStatementResidual>>,
    pub(super) health: Option<StatementNativeHealthFailure>,
}
impl<T> PropertyActionTail<T> {
    pub(super) fn new(cpu: Arc<FrontendPlanningCpuSource>) -> Self {
        Self {
            cpu,
            preparation: None,
            prepared: None,
            inner: None,
            inner_residual: None,
            health: None,
        }
    }
    pub(super) fn local_health(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.cpu.local_health_failure())
            .or_else(|| {
                self.inner
                    .as_ref()
                    .and_then(|action| action.local_health_failure())
            })
    }
    pub(super) fn start_operation(
        &mut self,
        operation: impl Future<
            Output = Result<Result<T, QueryServiceError>, StatementNativeHealthFailure>,
        > + Send
        + 'static,
    ) {
        assert!(self.preparation.is_none() && self.prepared.is_none());
        self.preparation = Some(Box::pin(operation));
    }

    pub(super) fn handoff(
        tail: &mut Option<Box<Self>>,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure>
    where
        T: Send + 'static,
    {
        if target.is_some() {
            return Err(StatementNativeHealthFailure::actual_exit_unproven(
                "property preparation residual receiver is occupied",
                Arc::new(WorkError::Conflict),
            ));
        }
        let original = tail
            .as_mut()
            .expect("original property preparation residual");
        if let Some(inner) = original.inner.as_mut() {
            inner.handoff_residual_in(&mut original.inner_residual)?;
        }
        *target = Some(tail.take().expect("one property preparation residual"));
        Ok(())
    }
    pub(super) fn poll_cpu_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(future) = self.preparation.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(result)) => self.prepared = Some(result),
                Poll::Ready(Err(health)) => self.health = Some(health),
            }
            drop(self.preparation.take());
        }
        if self.local_health().is_some() {
            return Poll::Pending;
        }
        match self.cpu.poll_closed(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(health)) => {
                self.health = Some(health);
                Poll::Pending
            }
            Poll::Ready(Ok(())) => Poll::Ready(()),
        }
    }
}
impl<T: Send + 'static> GovernedStatementResidual for PropertyActionTail<T> {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = self.local_health() {
            return Poll::Ready(Err(health));
        }
        let cpu = self.poll_cpu_closed(cx);
        if let Some(health) = self.local_health() {
            return Poll::Ready(Err(health));
        }
        std::task::ready!(cpu);
        if let Some(residual) = self.inner_residual.as_mut() {
            return residual.poll_retirement(cx);
        }
        Poll::Ready(Ok(()))
    }
}
