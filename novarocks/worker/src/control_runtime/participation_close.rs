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

//! One original Context close effect orders its physical fence with the
//! already admitted Task close cuts. This metadata grants no work or exit.

use crate::task_port::TaskPendingNotice;
use novarocks_execution_contract::TaskIdentity;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
pub(crate) struct ParticipationCloseGate {
    completed: AtomicBool,
    // One address per original Task close cut, not another obligation ledger.
    // The Context host drains notifications with its finite turn allowance.
    pending: Mutex<BTreeMap<TaskIdentity, Arc<dyn TaskPendingNotice>>>,
}
impl ParticipationCloseGate {
    pub(crate) fn stage(&self, task: TaskIdentity, pending: Arc<dyn TaskPendingNotice>) {
        assert!(
            self.pending
                .lock()
                .expect("original close notifications")
                .insert(task, pending)
                .is_none(),
            "one original Task close cut per Participation close effect"
        );
    }
    pub(crate) fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
    pub(crate) fn complete(&self) {
        self.completed.store(true, Ordering::Release);
    }
    pub(crate) fn notify_ready(&self, budget: usize) -> bool {
        if !self.is_completed() {
            return false;
        }
        for _ in 0..budget {
            let next = self
                .pending
                .lock()
                .expect("original close notifications")
                .pop_first();
            let Some((_, pending)) = next else {
                return false;
            };
            pending.pending();
        }
        !self
            .pending
            .lock()
            .expect("original close notifications")
            .is_empty()
    }
}
