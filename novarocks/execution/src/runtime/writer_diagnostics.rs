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

//! Private markers carried by the existing finite exchange-snapshot grant.
//! This contains only a copy of the original identity and its role observer,
//! never a Task, payload, runtime, resource receipt or scheduling capability.

use std::sync::Arc;

use novarocks_execution_contract::TaskIdentity;

use super::{exchange::TestEventObserver, runtime_state::RuntimeState};

#[derive(Clone)]
pub(crate) struct WriterDiagnostics {
    task: TaskIdentity,
    observer: Arc<dyn TestEventObserver>,
}

impl WriterDiagnostics {
    pub(crate) fn from_runtime(state: &RuntimeState) -> Option<Self> {
        let observer = state.exchange_test_observer()?;
        Some(Self {
            task: state.error_state().task_identity()?,
            observer,
        })
    }

    pub(crate) fn emit(&self, event: &str, sample: impl FnOnce() -> String) {
        if self.observer.is_active() {
            // Build samples before invoking the observer; no sampled owner
            // lock may remain held across this callback.
            let fields = sample();
            self.observer.observe(&format!(
                "{} event={event} task={} {fields}",
                novarocks_failpoint::observation_marker::WRITER_DIAGNOSTIC,
                self.task
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Debug)]
    struct Inactive;
    impl TestEventObserver for Inactive {
        fn is_active(&self) -> bool {
            false
        }
        fn observe(&self, _: &str) {
            panic!("an inactive scope cannot emit");
        }
    }
    #[test]
    fn an_observer_without_an_original_task_cannot_create_diagnostic_identity() {
        let state = RuntimeState::default().with_exchange_test_observer(Some(Arc::new(Inactive)));
        assert!(WriterDiagnostics::from_runtime(&state).is_none());
    }
}
