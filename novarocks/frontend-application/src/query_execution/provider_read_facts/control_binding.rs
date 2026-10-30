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

//! The original Provider completion Source binds actual selected read facts.
use super::ProviderReadError;
use novarocks_spi::connector::read_stack::ConnectorReadRequestControlFactory;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_workload_control::joint_admission::{
    ControlBindingProposal, JointAdjustmentReceipt, JointAdmission, JointProposalSlotWait,
    JointTryBegin,
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Inline in the existing ProviderCreatorSource, never in a borrowed F. Its
/// original fixed Source quote includes these fields; there is no new owner,
/// runtime, pool or registry. Each successful creator exits before reuse.
#[derive(Default)]
pub(super) struct ProviderCreatorControlBinding {
    proposal: Option<ControlBindingProposal>,
    admission: Option<JointAdmission>,
    receipt: Option<JointAdjustmentReceipt>,
    wait: Option<JointProposalSlotWait>,
    refusal: Option<ProviderReadError>,
    bound: bool,
    polling: bool,
}
impl ProviderCreatorControlBinding {
    pub(super) fn poll(
        &mut self,
        parent: &WorkScope,
        factory: &dyn ConnectorReadRequestControlFactory,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ProviderReadError>> {
        if let Some(error) = &self.refusal {
            return Poll::Ready(Err(error.clone()));
        }
        if self.bound {
            return Poll::Ready(Ok(()));
        }
        if self.polling {
            return Poll::Ready(Err(WorkError::Conflict.into()));
        }
        self.polling = true;
        let result = self.poll_original(parent, factory, cx);
        self.polling = false;
        if let Poll::Ready(Err(error)) = &result {
            self.refusal = Some(error.clone());
        }
        result
    }
    fn poll_original(
        &mut self,
        parent: &WorkScope,
        factory: &dyn ConnectorReadRequestControlFactory,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ProviderReadError>> {
        if let Some(wait) = self.wait.as_mut() {
            match Pin::new(wait).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(())) => drop(self.wait.take()),
            }
        }
        if self.admission.is_none() && self.receipt.is_none() {
            if self.proposal.is_none() {
                match parent.try_begin_control_binding()? {
                    JointTryBegin::Started(proposal) => self.proposal = Some(proposal),
                    JointTryBegin::Full(signal) => {
                        self.wait = Some(signal.into_wait());
                        match Pin::new(self.wait.as_mut().unwrap()).poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                            Poll::Ready(Ok(())) => {
                                drop(self.wait.take());
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                    }
                }
            }
            let mut refusal = None;
            let selected = factory.visit_planning_dependencies(&mut |fact| {
                self.proposal
                    .as_mut()
                    .unwrap()
                    .push_dependency(fact)
                    .map_err(|error| {
                        refusal = Some(error);
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "provider creator selected dependency was refused",
                        )
                    })
            });
            if let Some(error) = refusal {
                return Poll::Ready(Err(error.into()));
            }
            selected.map_err(ProviderReadError::from)?;
            self.proposal.as_mut().unwrap().end_change()?;
            self.admission = Some(
                self.proposal
                    .take()
                    .unwrap()
                    .submit()
                    .map_err(|e| e.error)?,
            );
        }
        if self.receipt.is_none() {
            let mut batch = match Pin::new(self.admission.as_mut().unwrap()).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.error.into())),
                Poll::Ready(Ok(batch)) => batch,
            };
            self.receipt = batch.adjustment.take();
            let empty = batch.new_claims.is_empty();
            drop(batch);
            drop(self.admission.take());
            if !empty {
                return Poll::Ready(Err(WorkError::Conflict.into()));
            }
        }
        let notification = self
            .receipt
            .as_mut()
            .ok_or(WorkError::Conflict)?
            .install_control_scope_in(parent)?;
        self.bound = true;
        // Exact parent binding is stable before arbitrary wake or original F /
        // disarmed receipt destruction. The child author still runs separately.
        notification.notify();
        drop(self.receipt.take());
        Poll::Ready(Ok(()))
    }
    pub(super) fn after_source_exit(&mut self) {
        drop(self.wait.take());
        drop(self.receipt.take());
        drop(self.admission.take());
        drop(self.proposal.take());
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests;
