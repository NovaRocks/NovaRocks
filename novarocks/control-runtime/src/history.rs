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

//! Optional bounded history; observations never participate in runtime policy.

use crate::sync::{Arc, Mutex};
use novarocks_control_runtime_core::{
    history::{HistoryBuffer, HistoryEntry, HistoryError, HistoryFact},
    identity::Tick,
};
struct CaptureState {
    buffer: HistoryBuffer,
    last_observed: Tick,
    incomplete: Option<HistoryError>,
}
#[derive(Clone)]
pub struct HistoryCapture {
    state: Arc<Mutex<CaptureState>>,
}
#[derive(Clone, Debug)]
pub struct HistorySlice {
    pub entries: Vec<HistoryEntry>,
    /// True means no observed facts were lost; it does not mean this slice
    /// drained the buffer or the enclosing execution has finished.
    pub complete: bool,
    pub remaining: usize,
    pub error: Option<HistoryError>,
}
impl HistoryCapture {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(CaptureState {
                buffer: HistoryBuffer::new(capacity),
                last_observed: Tick(0),
                incomplete: None,
            })),
        }
    }
    /// Explicit driver observations only. Async exit facts use the most recently
    /// observed tick; their ordinal records handoff order, not an exact wall time.
    pub fn observe_time(&self, now: Tick) {
        let mut state = self.state.lock().expect("runtime history lock");
        state.last_observed = state.last_observed.max(now);
    }
    pub(crate) fn record(&self, fact: HistoryFact) {
        let mut state = self.state.lock().expect("runtime history lock");
        if state.incomplete.is_some() {
            return;
        }
        let now = state.last_observed;
        if let Err(error) = state.buffer.record(now, fact) {
            state.incomplete = Some(error);
        }
    }
    pub fn drain(&self, budget: usize) -> HistorySlice {
        let mut state = self.state.lock().expect("runtime history lock");
        let mut entries = Vec::new();
        for _ in 0..budget {
            let Some(entry) = state.buffer.pop() else {
                break;
            };
            entries.push(entry);
        }
        HistorySlice {
            entries,
            complete: state.incomplete.is_none(),
            remaining: state.buffer.len(),
            error: state.incomplete,
        }
    }
    pub fn is_complete(&self) -> bool {
        self.state
            .lock()
            .expect("runtime history lock")
            .incomplete
            .is_none()
    }
}
