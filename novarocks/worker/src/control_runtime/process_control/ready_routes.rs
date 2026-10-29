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

//! Coalesced runnable Context addresses in actual readiness order. The
//! existing pure fair queue stores a unit marker, never Work or an RPC owner.

use novarocks_control_runtime_core::fair_queue::{FairQueueError, FairWorkQueue};
use novarocks_execution_contract::QueryContextRef;
use std::num::NonZeroUsize;

pub(super) struct ReadyRoutes {
    queue: FairWorkQueue<QueryContextRef, ()>,
}
impl ReadyRoutes {
    pub(super) fn new(request_positions: NonZeroUsize) -> Self {
        // Every runnable Context retains at least one original pending R.
        // This is the existing admitted-R bound, not a separate quota.
        let positions = request_positions.get();
        Self {
            queue: FairWorkQueue::new(positions, positions, 1),
        }
    }
    pub(super) fn mark(&mut self, context: QueryContextRef) {
        match self.queue.enqueue(context, ()) {
            Ok(()) | Err(FairQueueError::DuplicateWork) => {}
            Err(error) => {
                panic!("Root route readiness exceeded its original R inventory: {error:?}")
            }
        }
    }
    pub(super) fn forget(&mut self, context: QueryContextRef) {
        match self.queue.remove_queued(context, ()) {
            Ok(()) | Err(FairQueueError::UnknownWork) => {}
            Err(error) => panic!("Root route readiness lost its original marker: {error:?}"),
        }
    }
    pub(super) fn pop(&mut self) -> Option<QueryContextRef> {
        let (context, ()) = self.queue.pop()?;
        // Finish only the selected metadata marker before the owner attempts
        // its route. A still-runnable owner is re-enqueued at the tail. This
        // neither starts nor exits the original R, source, callback or Work.
        self.queue
            .finish(context, ())
            .expect("the selected readiness marker exists");
        Some(context)
    }
    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
#[path = "ready_routes_tests.rs"]
mod tests;
