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

//! Physical destruction of a never-loaded capture in its original paid R.

use crate::control_runtime::{
    loading::TaskCreatePayload, request_endpoint::RequestNotice, task::TaskHostBinding,
};
use novarocks_control_runtime::owned_return::OwnedReturnSender;
use novarocks_control_runtime_core::identity::{ScopeId, WorkId};
use std::mem::ManuallyDrop;

pub(crate) struct TaskCreateDisposalJob {
    key: (ScopeId, WorkId),
    payload: Option<Box<ManuallyDrop<(TaskCreatePayload, TaskHostBinding)>>>,
    sender: Option<OwnedReturnSender<()>>,
    notice: RequestNotice,
}
impl TaskCreateDisposalJob {
    pub(super) fn new(
        scope: ScopeId,
        work: WorkId,
        payload: ManuallyDrop<TaskCreatePayload>,
        binding: TaskHostBinding,
        sender: OwnedReturnSender<()>,
        notice: RequestNotice,
    ) -> Self {
        Self {
            key: (scope, work),
            payload: Some(Box::new(ManuallyDrop::new((
                ManuallyDrop::into_inner(payload),
                binding,
            )))),
            sender: Some(sender),
            notice,
        }
    }
    pub(crate) fn queue_key(&self) -> (ScopeId, WorkId) {
        self.key
    }
    pub(crate) fn run(mut self) {
        let mut payload = self.payload.take().expect("original never-loaded Create");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // SAFETY: this stable original capture is destroyed once on the
            // physical worker. An unwind never moves or retries its contents.
            unsafe { ManuallyDrop::drop(payload.as_mut()) };
        }));
        if let Err(panic) = result {
            std::mem::forget(payload);
            // Losing the original sender records Unknown, never an exit.
            drop(self.sender.take());
            self.notice.notify();
            std::panic::resume_unwind(panic);
        }
        drop(payload);
        if let Err(refusal) = self
            .sender
            .take()
            .expect("original Create return")
            .publish(())
        {
            tracing::error!(error = ?refusal.error, "Original Create disposal return failed");
            std::mem::forget(refusal);
            std::process::abort();
        }
        self.notice.notify();
    }
}
impl Drop for TaskCreateDisposalJob {
    fn drop(&mut self) {
        if let Some(payload) = self.payload.take() {
            // A lost job retains the opaque original and marks its existing
            // return Unknown. No foreign destructor runs under a driver lock.
            std::mem::forget(payload);
            drop(self.sender.take());
            self.notice.notify();
        }
    }
}
