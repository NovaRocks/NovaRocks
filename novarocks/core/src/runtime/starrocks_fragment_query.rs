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

//! Narrow query-runtime services consumed by the StarRocks compat adapter.
//!
//! The facade keeps `QueryContextManager` and its generation bookkeeping inside
//! core while letting the compat adapter own admission, handoff, report, and
//! cleanup sequencing around the protocol-neutral fragment kernel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use novarocks_spi::connector::ConnectorCancellation;
use novarocks_types::QueryId;

use crate::cache::CacheOptions;
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::row_position::RowPositionDescriptor;
use crate::runtime::descriptor_snapshot::DescriptorSnapshot;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::query_context::{
    FragmentFinishReportDecision, QueryContextManager, QueryExecutionKey, StarRocksQueryGeneration,
    StarRocksQueryHandoff, query_context_manager,
};

pub use crate::runtime::query_context::{LookupFetcherLifecycle, QueryCleanupLease};

#[derive(Clone)]
pub struct StarRocksFragmentQueryRuntime {
    manager: Arc<QueryContextManager>,
}

struct StarRocksConnectorCancellation {
    manager: Arc<QueryContextManager>,
    query_id: QueryId,
}

impl ConnectorCancellation for StarRocksConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.manager.is_query_canceled(self.query_id)
    }
}

impl StarRocksFragmentQueryRuntime {
    pub fn new() -> Self {
        Self {
            manager: query_context_manager(),
        }
    }

    /// Returns the read-only cancellation capability needed by compat-owned
    /// connector decode. The query manager itself never crosses this facade.
    pub fn connector_cancellation(&self, query_id: QueryId) -> Arc<dyn ConnectorCancellation> {
        Arc::new(StarRocksConnectorCancellation {
            manager: Arc::clone(&self.manager),
            query_id,
        })
    }

    #[cfg(test)]
    fn from_manager_for_test(manager: Arc<QueryContextManager>) -> Self {
        Self { manager }
    }

    pub fn prepare_admission(
        &self,
        query_id: QueryId,
        delivery_expire: Duration,
        query_expire: Duration,
        cache_options: CacheOptions,
    ) -> Result<StarRocksFragmentAdmission, String> {
        let query_mem_tracker = self.manager.prepare_starrocks_admission(
            query_id,
            delivery_expire,
            query_expire,
            cache_options,
        )?;
        Ok(StarRocksFragmentAdmission {
            runtime: self.clone(),
            query_id,
            query_mem_tracker,
            active: true,
        })
    }

    pub fn commit_handoff(
        &self,
        handoff: StarRocksFragmentHandoff,
        make_cleanup_lease: impl FnOnce() -> Option<QueryCleanupLease>,
    ) -> Result<StarRocksFragmentCommittedHandoff, String> {
        let execution = handoff.execution;
        let fragment_instance_ids = handoff
            .inner
            .instances
            .iter()
            .map(|(finst_id, _)| *finst_id)
            .collect();
        let query_mem_tracker = self
            .manager
            .commit_starrocks_handoff(handoff.inner, make_cleanup_lease)?;
        Ok(StarRocksFragmentCommittedHandoff {
            query_mem_tracker,
            pre_start: StarRocksFragmentPreStartHandoff {
                runtime: self.clone(),
                execution,
                fragment_instance_ids,
                active: true,
            },
        })
    }

    pub fn incremental_change_op_slot(
        &self,
        fragment_instance_id: UniqueId,
        node_id: i32,
    ) -> Result<Option<SlotId>, String> {
        self.manager
            .incremental_change_op_slot(fragment_instance_id, node_id)
    }

    pub fn append_incremental_scan_ranges(
        &self,
        fragment_instance_id: UniqueId,
        node_id: i32,
        ranges: Vec<crate::exec::node::scan::IncrementalScanRange>,
    ) -> Result<(), String> {
        self.manager
            .append_incremental_scan_ranges(fragment_instance_id, node_id, ranges)
    }

    pub fn cancel_query(
        &self,
        execution: StarRocksFragmentExecution,
        error: String,
    ) -> Vec<UniqueId> {
        self.manager.cancel_query_execution(execution.inner, error)
    }

    pub fn finish_fragment_for_report(
        &self,
        execution: StarRocksFragmentExecution,
    ) -> StarRocksFragmentReportDecision {
        StarRocksFragmentReportDecision {
            inner: self
                .manager
                .finish_fragment_for_report_execution(execution.inner),
        }
    }

    pub fn unregister_fragment(
        &self,
        fragment_instance_id: UniqueId,
        execution: StarRocksFragmentExecution,
    ) {
        self.manager
            .unregister_finst_execution(fragment_instance_id, execution.inner);
    }

    pub fn cleanup_after_fragment_report(
        &self,
        query_id: QueryId,
        decision: StarRocksFragmentReportDecision,
    ) {
        self.manager
            .cleanup_after_fragment_report(query_id, decision.inner);
    }

    pub fn finish_fragment(&self, execution: StarRocksFragmentExecution) {
        self.manager.finish_fragment_execution(execution.inner);
    }
}

pub struct StarRocksFragmentAdmission {
    runtime: StarRocksFragmentQueryRuntime,
    query_id: QueryId,
    query_mem_tracker: Arc<MemTracker>,
    active: bool,
}

impl StarRocksFragmentAdmission {
    pub fn query_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.query_mem_tracker)
    }

    pub fn fragment_mem_tracker(&self, fragment_instance_id: UniqueId) -> Arc<MemTracker> {
        MemTracker::new_child(
            format!(
                "fragment_{:x}_{:x}",
                fragment_instance_id.high(),
                fragment_instance_id.low()
            ),
            &self.query_mem_tracker,
        )
    }
}

impl Drop for StarRocksFragmentAdmission {
    fn drop(&mut self) {
        if self.active {
            self.runtime
                .manager
                .release_starrocks_admission(self.query_id);
            self.active = false;
        }
    }
}

#[derive(Clone, Copy)]
pub struct StarRocksFragmentExecution {
    inner: QueryExecutionKey,
}

impl StarRocksFragmentExecution {
    pub const fn query_id(self) -> QueryId {
        self.inner.query_id()
    }
}

pub struct StarRocksFragmentHandoff {
    inner: StarRocksQueryHandoff,
    execution: StarRocksFragmentExecution,
}

pub struct StarRocksFragmentCommittedHandoff {
    query_mem_tracker: Arc<MemTracker>,
    pre_start: StarRocksFragmentPreStartHandoff,
}

impl StarRocksFragmentCommittedHandoff {
    pub fn query_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.query_mem_tracker)
    }

    pub fn into_pre_start(self) -> StarRocksFragmentPreStartHandoff {
        self.pre_start
    }
}

pub struct StarRocksFragmentPreStartHandoff {
    runtime: StarRocksFragmentQueryRuntime,
    execution: StarRocksFragmentExecution,
    fragment_instance_ids: Vec<UniqueId>,
    active: bool,
}

impl StarRocksFragmentPreStartHandoff {
    pub const fn execution(&self) -> StarRocksFragmentExecution {
        self.execution
    }

    pub fn start(mut self) -> StarRocksFragmentExecution {
        self.active = false;
        self.execution
    }

    pub fn rollback(mut self) -> bool {
        self.rollback_inner()
    }

    fn rollback_inner(&mut self) -> bool {
        if !self.active {
            return false;
        }
        self.active = false;
        self.runtime
            .manager
            .rollback_starrocks_handoff(self.execution.inner, &self.fragment_instance_ids)
    }
}

impl Drop for StarRocksFragmentPreStartHandoff {
    fn drop(&mut self) {
        if self.active {
            let rolled_back = self.rollback_inner();
            debug_assert!(
                rolled_back,
                "dropped pre-start StarRocks handoff must still own its exact query routes"
            );
        }
    }
}

impl StarRocksFragmentHandoff {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_id: QueryId,
        generation: u64,
        delivery_expire: Duration,
        query_expire: Duration,
        cache_options: CacheOptions,
        descriptor_snapshot: Option<Arc<DescriptorSnapshot>>,
        total_fragments: Option<usize>,
        row_pos_descs: HashMap<i32, RowPositionDescriptor>,
        lookup_fetchers: HashMap<i32, LookupFetcherLifecycle>,
        instances: Vec<(UniqueId, HashMap<i32, Option<SlotId>>)>,
    ) -> Result<Self, String> {
        let generation = StarRocksQueryGeneration::new(generation)?;
        let execution = QueryExecutionKey::starrocks(query_id, generation);
        Ok(Self {
            inner: StarRocksQueryHandoff {
                execution,
                delivery_expire,
                query_expire,
                fragment_count: instances.len(),
                cache_options,
                descriptor_snapshot,
                total_fragments,
                row_pos_descs,
                lookup_fetchers,
                instances,
            },
            execution: StarRocksFragmentExecution { inner: execution },
        })
    }

    pub const fn execution(&self) -> StarRocksFragmentExecution {
        self.execution
    }

    pub const fn query_id(&self) -> QueryId {
        self.execution.query_id()
    }

    pub const fn delivery_expire(&self) -> Duration {
        self.inner.delivery_expire
    }

    pub const fn query_expire(&self) -> Duration {
        self.inner.query_expire
    }

    pub fn cache_options(&self) -> CacheOptions {
        self.inner.cache_options.clone()
    }
}

pub struct StarRocksFragmentReportDecision {
    inner: FragmentFinishReportDecision,
}

impl StarRocksFragmentReportDecision {
    pub const fn include_runtime_filter_profile(&self) -> bool {
        self.inner.include_runtime_filter_profile
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::cache::CacheOptions;
    use crate::common::types::UniqueId;
    use crate::runtime::query_context::{QueryContextManager, QueryId};

    use super::{StarRocksFragmentHandoff, StarRocksFragmentQueryRuntime};

    fn handoff(query_id: QueryId, generation: u64, finst_id: UniqueId) -> StarRocksFragmentHandoff {
        StarRocksFragmentHandoff::new(
            query_id,
            generation,
            Duration::from_secs(30),
            Duration::from_secs(60),
            CacheOptions::from_query_options(None).expect("default cache options"),
            None,
            Some(1),
            HashMap::new(),
            HashMap::new(),
            vec![(finst_id, HashMap::new())],
        )
        .expect("test handoff")
    }

    #[test]
    fn rolling_back_a_pre_start_handoff_preserves_an_already_started_query() {
        let manager = QueryContextManager::new_for_test();
        let runtime = StarRocksFragmentQueryRuntime::from_manager_for_test(Arc::clone(&manager));
        let started_query = QueryId::new(91_001, 91_002);
        let started_finst = UniqueId::new(91_003, 91_004);
        let rollback_query = QueryId::new(91_005, 91_006);
        let rollback_finst = UniqueId::new(91_007, 91_008);

        let started = runtime
            .commit_handoff(handoff(started_query, 1, started_finst), || None)
            .expect("started handoff");
        let started_execution = started.into_pre_start().start();
        let rollback = runtime
            .commit_handoff(handoff(rollback_query, 1, rollback_finst), || None)
            .expect("rollback handoff");

        assert!(rollback.into_pre_start().rollback());
        assert_eq!(
            manager.query_execution_by_finst(started_finst),
            Some(started_execution.inner),
            "rolling back another pre-start lease must not delete a started query"
        );
        assert_eq!(manager.query_execution_by_finst(rollback_finst), None);
    }
}
