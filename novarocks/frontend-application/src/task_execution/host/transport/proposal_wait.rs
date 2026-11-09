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

//! One actual proposal-slot observer belongs to one prepaid job Source.
//! There is no fixed waiter quota or repeated registration on a full poll.
use novarocks_workload_control::{
    WorkError,
    joint_admission::{JointProposalSlotSignal, JointProposalSlotWait},
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
#[derive(Default)]
pub(crate) struct ProposalWait {
    wait: Option<JointProposalSlotWait>,
}
impl ProposalWait {
    /// Only this boxed named WC future is separately allocated. The inline
    /// wait/signal/trait-object handles already belong to the containing F.
    pub(crate) fn owned_future_layout() -> std::alloc::Layout {
        JointProposalSlotWait::metadata_layouts().1
    }

    pub(crate) fn poll(
        &mut self,
        signal: &JointProposalSlotSignal,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        if self
            .wait
            .as_ref()
            .is_some_and(|wait| !wait.is_for_signal(signal))
        {
            return Poll::Ready(Err(WorkError::Conflict));
        }
        if self.wait.is_none() {
            self.wait = Some(signal.clone().into_wait());
        }
        let polled = Pin::new(
            self.wait
                .as_mut()
                .expect("same original Source proposal observer"),
        )
        .poll(cx);
        if polled.is_ready() {
            drop(self.wait.take());
        }
        polled
    }
}
