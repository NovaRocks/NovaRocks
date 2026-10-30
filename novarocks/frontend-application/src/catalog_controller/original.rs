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

//! One Management Root for each actual catalog inventory round.
//! The existing cadence only borrows this stable round across every await.
use super::*;
use crate::{
    query_execution::maintenance::io_admission::MetadataIoAdmission,
    state_store::execution::{StateStoreOriginalBinding, StateStoreOriginalOwner},
};
use novarocks_catalog_application::CatalogApplicationError;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_workload_control::{
    CancellationReason, RootAdmissionHandle, RootWork, WorkClass, WorkRequest,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

type Scan = Pin<Box<dyn Future<Output = Result<(), CatalogApplicationError>> + Send>>;
pub(super) struct CatalogOriginalRounds {
    admission: RootAdmissionHandle,
    binding: StateStoreOriginalBinding,
    round: tokio::sync::Mutex<Option<Round>>,
}
struct Round {
    root: Option<RootWork>,
    io: Option<Arc<MetadataIoAdmission>>,
    original: Option<StateStoreOriginalOwner>,
    future: Option<Scan>,
    outcome: Option<Result<(), CatalogApplicationError>>,
    terminal: Option<Result<(), String>>,
    failure: Option<SharedFailureCause>,
    stopped: bool,
    started: bool,
    closed: bool,
}
impl CatalogOriginalRounds {
    pub(super) fn new(admission: RootAdmissionHandle, binding: StateStoreOriginalBinding) -> Self {
        Self {
            admission,
            binding,
            round: tokio::sync::Mutex::new(None),
        }
    }
    pub(super) async fn scan(&self, controller: &FrontendCatalogController) -> Result<(), String> {
        let mut slot = self.round.lock().await;
        if slot.is_none() {
            *slot = Some(Round {
                root: None,
                io: None,
                original: None,
                future: None,
                outcome: None,
                terminal: None,
                failure: None,
                stopped: false,
                started: false,
                closed: false,
            });
        }
        // Publication into the original round precedes arbitrary authority wakes.
        let round = slot.as_mut().unwrap();
        if round.root.is_none() {
            self.admission
                .try_begin_root_in(WorkRequest::new(WorkClass::Management), &mut round.root)
                .map_err(|error| error.to_string())?;
        }
        if round.terminal.is_none() {
            let result = std::future::poll_fn(|cx| round.poll(controller, &self.binding, cx)).await;
            round.terminal = Some(result);
        }
        if round.terminal.as_ref().is_some_and(Result::is_err) && !round.closed {
            if round
                .original
                .as_ref()
                .and_then(StateStoreOriginalOwner::local_health_failure)
                .is_some()
            {
                controller.request_stop_for_process_exit();
            } else {
                round.stop();
                std::future::poll_fn(|cx| round.poll_close(cx)).await?;
            }
        }
        let result = round
            .terminal
            .as_ref()
            .expect("stable catalog round result")
            .clone();
        if round.closed {
            let mut done = slot.take().expect("same actual catalog round");
            // Drop every scope/result alias before recording real completion.
            drop(done.future.take());
            drop(done.original.take());
            drop(done.io.take());
            if let Some(root) = done.root.take() {
                drop(root.business);
                root.owner.complete_after_terminal_cancel_settled();
            }
        }
        result
    }
    pub(super) async fn close(&self) -> Result<(), String> {
        let mut slot = self.round.lock().await;
        let Some(round) = slot.as_mut() else {
            return Ok(());
        };
        round.stop();
        std::future::poll_fn(|cx| round.poll_close(cx)).await?;
        if round.closed {
            let mut done = slot.take().unwrap();
            drop(done.future.take());
            drop(done.original.take());
            drop(done.io.take());
            if let Some(root) = done.root.take() {
                drop(root.business);
                root.owner.complete_after_terminal_cancel_settled();
            }
        }
        Ok(())
    }
}
impl Round {
    fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        if let Some(root) = &self.root {
            root.owner.cancel(CancellationReason::ServerShutdown);
        }
        if let Some(original) = &self.original {
            original.request_close();
        }
    }
    fn progress(&mut self, cx: &mut Context<'_>) {
        if let Some(original) = &self.original
            && let Err(cause) = original.poll_progress(cx)
        {
            self.failure.get_or_insert(cause);
        }
        if let Some(future) = self.future.as_mut()
            && let Poll::Ready(outcome) = future.as_mut().poll(cx)
        {
            self.outcome = Some(outcome);
            drop(self.future.take());
        }
        if let Some(original) = &self.original
            && let Err(cause) = original.poll_progress(cx)
        {
            self.failure.get_or_insert(cause);
        }
    }
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), String>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if let Some(cause) = self
            .original
            .as_ref()
            .and_then(StateStoreOriginalOwner::local_health_failure)
        {
            return Poll::Ready(Err(cause.to_string()));
        }
        if let Some(original) = &self.original {
            original.request_close();
        }
        self.progress(cx);
        if self.future.is_some() {
            return Poll::Pending;
        }
        if let Some(original) = self.original.as_mut() {
            std::task::ready!(original.poll_closed(cx)).map_err(|cause| cause.to_string())?;
        }
        if let Some(io) = &self.io {
            let withdrawn = io
                .withdraw_unstarted_in()
                .map_err(|error| error.to_string())?;
            if withdrawn.is_some() {
                self.closed = true;
                return Poll::Ready(Ok(()));
            }
            let _ = io.poll_continuing(cx);
            std::task::ready!(io.poll_actual_quiescent_continuing(cx))
                .map_err(|error| error.detail().to_string())?;
        }
        self.closed = true;
        Poll::Ready(Ok(()))
    }
    fn poll(
        &mut self,
        controller: &FrontendCatalogController,
        binding: &StateStoreOriginalBinding,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), String>> {
        if controller.stopping.load(Ordering::Acquire) {
            self.stop();
        }
        if self.stopped {
            std::task::ready!(self.poll_close(cx))?;
            return Poll::Ready(Err("catalog round was stopped".into()));
        }
        if self.io.is_none() {
            self.io = Some(
                MetadataIoAdmission::for_original_source(
                    &self.root.as_ref().unwrap().owner,
                    binding.blocking_dependency_facts().to_vec(),
                )
                .map_err(|error| error.to_string())?,
            );
        }
        if !self.started {
            if !self
                .io
                .as_ref()
                .unwrap()
                .poll_progress(&self.root.as_ref().unwrap().owner, cx)
                .map_err(|error| error.to_string())?
            {
                return Poll::Pending;
            }
            let original = binding
                .for_root(self.root.as_ref().unwrap().owner.scope())
                .map_err(|cause| cause.to_string())?;
            let context = original
                .read_execution(controller.store.limits().transaction_deadline)
                .map_err(|cause| cause.to_string())?;
            self.original = Some(original);
            let projection = controller.projection.clone();
            let page_size = controller.config.page_size;
            let workers = controller.config.worker_count;
            self.future = Some(Box::pin(async move {
                projection
                    .reconcile_snapshot_in(page_size, workers, &context)
                    .await
                    .map(|_| ())
            }));
            self.started = true;
        }
        self.progress(cx);
        if let Some(cause) = self
            .original
            .as_ref()
            .and_then(StateStoreOriginalOwner::local_health_failure)
        {
            self.failure.get_or_insert(cause.clone());
            controller.request_stop_for_process_exit();
            return Poll::Ready(Err(cause.to_string()));
        }
        if self.future.is_some() {
            return Poll::Pending;
        }
        std::task::ready!(self.poll_close(cx))?;
        let outcome = self
            .outcome
            .as_ref()
            .expect("actual catalog snapshot result");
        if outcome.is_ok() {
            controller.metrics.resyncs.fetch_add(1, Ordering::Relaxed);
            controller.publish_metrics();
        }
        Poll::Ready(
            outcome
                .as_ref()
                .map(|_| ())
                .map_err(|error| error.to_string()),
        )
    }
}
