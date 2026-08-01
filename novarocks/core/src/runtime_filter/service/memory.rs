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

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use crate::common::types::UniqueId;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime_filter::port::events::RuntimeFilterEventSink;
use crate::runtime_filter::port::support::{
    MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
};

use super::RuntimeFilterService;

struct SystemRuntimeFilterClock;

impl RuntimeFilterClock for SystemRuntimeFilterClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

pub(crate) struct MemTrackerMemoryAccount {
    tracker: Arc<MemTracker>,
    reserved: AtomicI64,
}

impl MemTrackerMemoryAccount {
    pub(crate) fn new(parent: &Arc<MemTracker>) -> Arc<Self> {
        Arc::new(Self {
            tracker: MemTracker::new_child("runtime_filter_service", parent),
            reserved: AtomicI64::new(0),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_root_for_test(label: &str) -> Arc<Self> {
        Self::new(&MemTracker::new_root(label))
    }

    #[cfg(test)]
    pub(crate) fn current(&self) -> i64 {
        self.tracker.current()
    }

    #[cfg(test)]
    pub(crate) fn peak(&self) -> i64 {
        self.tracker.peak()
    }
}

impl RuntimeFilterMemoryAccount for MemTrackerMemoryAccount {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
        let bytes = i64::try_from(bytes).map_err(|_| MemoryAccountError::CapacityExceeded)?;
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(bytes)
                .ok_or(MemoryAccountError::CapacityExceeded)?;
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.tracker.consume(bytes);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        if let Ok(bytes) = i64::try_from(bytes) {
            self.reserved.fetch_sub(bytes, Ordering::AcqRel);
            self.tracker.release(bytes);
        }
    }
}

impl RuntimeFilterService {
    pub(crate) fn new_for_query(
        query_id: UniqueId,
        event_sink: Arc<dyn RuntimeFilterEventSink>,
        query_mem_tracker: &Arc<MemTracker>,
    ) -> Self {
        Self::new_with_dependencies(
            query_id,
            Arc::new(SystemRuntimeFilterClock),
            event_sink,
            MemTrackerMemoryAccount::new(query_mem_tracker),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use crate::common::types::UniqueId;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::install::{
        RuntimeFilterInstallView, local_participant_install_for_test,
    };
    use crate::runtime_filter::port::producer::InstallOutcome;
    use crate::runtime_filter::port::support::RuntimeFilterMemoryAccount;

    use super::MemTrackerMemoryAccount;
    use super::RuntimeFilterService;

    #[derive(Default)]
    struct Events(Mutex<Vec<RuntimeFilterEvent>>);

    impl RuntimeFilterEventSink for Events {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    #[test]
    fn mem_tracker_adapter_rejects_unrepresentable_bytes_without_mutation() {
        let account = MemTrackerMemoryAccount::new_root_for_test("overflow-test");
        assert!(account.try_consume(usize::MAX).is_err());
        assert_eq!(account.current(), 0);
        assert_eq!(account.peak(), 0);
    }

    #[test]
    fn query_constructor_creates_an_inert_service_and_labeled_child_tracker() {
        let parent = MemTracker::new_root("query");
        let events = Arc::new(Events::default());
        let service =
            RuntimeFilterService::new_for_query(UniqueId::new(1, 2), events.clone(), &parent);
        let empty = local_participant_install_for_test(RuntimeFilterInstallView::new(
            DeploymentEpoch::new(1),
            RuntimeFilterParticipantId::new(1),
            BTreeMap::new(),
        ));
        assert_eq!(
            service.install(empty).unwrap(),
            InstallOutcome::IgnoredEmpty
        );
        assert!(events.0.lock().unwrap().is_empty());
        let children = parent.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].label(), "runtime_filter_service");
        assert_eq!(children[0].current(), 0);
    }
}
