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
//! A finite statement/Task action at the existing build publication point.
use crate::test_controls::{NativeFaultClaim, NativeTestControls};
use novarocks_execution::runtime::{
    join_test_control::JoinBuildPublicationControl,
    task::{ExecutionFailure, ExecutionResult},
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_failpoint::QueryLifecycleFaultKind;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

pub(crate) struct JoinBuildHold {
    identity: TaskIdentity,
    controls: Arc<NativeTestControls>,
    claim: Mutex<Option<NativeFaultClaim>>,
    held: AtomicBool,
}
impl JoinBuildHold {
    pub(crate) fn new(identity: TaskIdentity, controls: Arc<NativeTestControls>) -> Self {
        Self {
            identity,
            controls,
            claim: Mutex::new(None),
            held: AtomicBool::new(false),
        }
    }
}
fn evidence(detail: impl std::fmt::Display) -> ExecutionFailure {
    novarocks_functions::KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(
        &format!("join publication test control: {detail}"),
    ))
    .into()
}
impl JoinBuildPublicationControl for JoinBuildHold {
    fn before_publish(&self, node: usize) -> ExecutionResult<bool> {
        let mut claim = self
            .claim
            .lock()
            .map_err(|_| evidence("claim lock failed"))?;
        if claim.is_none() {
            let Some(selected) = self
                .controls
                .claim_task(
                    QueryLifecycleFaultKind::JoinBuildBeforePublishHold,
                    self.identity,
                    true,
                )
                .map_err(evidence)?
            else {
                return Ok(true);
            };
            selected.record_hit().map_err(evidence)?;
            tracing::info!(task=%self.identity,node,"NOVAROCKS_JOIN_BUILD_PUBLICATION_HELD");
            *claim = Some(selected);
        }
        let selected = claim.as_ref().expect("claimed publication action");
        if !selected.cancellation().is_active() {
            return Err(novarocks_functions::KernelFailure::Cancelled.into());
        }
        let open = selected.action_ended().map_err(evidence)?;
        self.held.store(!open, Ordering::Release);
        Ok(open)
    }
    fn is_held(&self) -> bool {
        self.held.load(Ordering::Acquire)
    }
}
