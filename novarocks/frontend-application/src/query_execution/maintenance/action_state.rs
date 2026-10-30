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

//! Original maintenance action row; borrowed waits never own its only call.

use super::*;
use crate::connector::metadata_maintenance::MetadataMaintenanceCall;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// One fixed state in the original statement/action, not a process registry.
/// A Product future may clone this same row alias; it cannot replace the row.
#[derive(Clone)]
pub struct MaintenanceActionState {
    original: Arc<Mutex<Option<ActionBody>>>,
}
#[derive(Default)]
struct ActionBody {
    engine: Option<RequestScopedMaintenanceEngine>,
    call: Option<MetadataMaintenanceCall>,
}
impl Default for MaintenanceActionState {
    fn default() -> Self {
        Self {
            original: Arc::new(Mutex::new(Some(ActionBody::default()))),
        }
    }
}
struct ActionLoan<'a> {
    source: &'a MaintenanceActionState,
    body: Option<ActionBody>,
}
impl Drop for ActionLoan<'_> {
    fn drop(&mut self) {
        let mut source = self
            .source
            .original
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert!(source.is_none(), "same maintenance row was loaned once");
        *source = self.body.take();
    }
}
impl MaintenanceActionState {
    pub(crate) fn publication_disposition(
        &self,
    ) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
        let loan = self.loan().ok()?;
        loan.body.as_ref()?.call.as_ref()?.publication_disposition()
    }

    pub(super) fn has_applied_publication(&self) -> Result<bool, String> {
        let loan = self.loan()?;
        Ok(loan
            .body
            .as_ref()
            .unwrap()
            .call
            .as_ref()
            .is_some_and(|call| call.has_applied_publication()))
    }

    pub(super) fn needs_cleanup_custody(&self) -> Result<bool, String> {
        let loan = self.loan()?;
        Ok(loan
            .body
            .as_ref()
            .unwrap()
            .call
            .as_ref()
            .is_some_and(|call| {
                call.cleanup_responsibility()
                    == novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
            }))
    }
    /// Closed moves only. The caller installs the receiving Box in the same
    /// stable Source before disposing execution/result-window aliases.
    pub(super) fn take_cleanup_call_in(
        &self,
        target: &mut Option<MetadataMaintenanceCall>,
    ) -> Result<(), String> {
        if target.is_some() {
            return Err("metadata residual receiver is occupied".into());
        }
        let mut loan = self.loan()?;
        *target = loan.body.as_mut().unwrap().call.take();
        Ok(())
    }
    pub(super) fn drop_execution_aliases(&self) -> Result<(), String> {
        let mut loan = self.loan()?;
        let engine = loan.body.as_mut().unwrap().engine.take();
        // Restore the canonical row before any result-window last-alias Drop.
        drop(loan);
        drop(engine);
        Ok(())
    }

    fn loan(&self) -> Result<ActionLoan<'_>, String> {
        let body = self
            .original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .ok_or("maintenance action is already being polled")?;
        Ok(ActionLoan {
            source: self,
            body: Some(body),
        })
    }
    pub(super) fn has_engine(&self) -> Result<bool, String> {
        let original = self.original.lock().unwrap_or_else(|p| p.into_inner());
        Ok(original
            .as_ref()
            .ok_or("maintenance action is already being polled")?
            .engine
            .is_some())
    }
    pub(super) fn install_engine(
        &self,
        engine: RequestScopedMaintenanceEngine,
    ) -> Result<(), String> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        // Retain the first exact execution/context, including after cancellation.
        if body.engine.is_none() {
            body.engine = Some(engine);
        }
        Ok(())
    }
    pub(super) async fn execute(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        std::future::poll_fn(|cx| self.poll_execute(request.clone(), cx)).await
    }
    fn poll_execute(
        &self,
        request: MaintenanceActionRequest,
        cx: &mut Context<'_>,
    ) -> Poll<Result<MaintenanceActionOutcome, String>> {
        let mut loan = match self.loan() {
            Ok(loan) => loan,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let body = loan.body.as_mut().unwrap();
        let engine = body
            .engine
            .as_ref()
            .expect("original exact engine installed");
        // Only this borrowed tail is reconstructed: all I/O F/lease/session/
        // outcome live in body.call. The whole Product F/gate is NOT rebuilt.
        // The inner Future drops before the loan restores the original call.
        {
            let mut borrowed = std::pin::pin!(super::iceberg::execute_action_with_ports(
                &mut body.call,
                engine.kernel.connector_control().as_ref(),
                &engine.kernel,
                request,
                engine.connector_context.clone(),
            ));
            borrowed.as_mut().poll(cx)
        }
    }

    pub(super) async fn execute_automatic(
        &self,
        request: MaintenanceActionRequest,
        effect: MaintenanceEffectId,
    ) -> Result<novarocks_table_maintenance::AutomaticMaintenanceOutcome, TerminalError> {
        std::future::poll_fn(|cx| {
            let mut loan = match self.loan() {
                Ok(loan) => loan,
                Err(error) => return Poll::Ready(Err(TerminalError::pre_dispatch_failed(error))),
            };
            let body = loan.body.as_mut().unwrap();
            let engine = body
                .engine
                .as_ref()
                .expect("original exact engine installed");
            let mut borrowed = std::pin::pin!(
                super::iceberg::execute_automatic_metadata_action_with_ports(
                    &mut body.call,
                    engine.kernel.connector_control().as_ref(),
                    &engine.kernel,
                    request.clone(),
                    effect,
                    engine.connector_context.clone(),
                )
            );
            borrowed.as_mut().poll(cx)
        })
        .await
    }
}
