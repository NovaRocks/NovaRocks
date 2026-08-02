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

use std::collections::BTreeSet;
use std::sync::Arc;

use novarocks::query_execution::lifecycle::{QueryExecutionId, QueryTerminationReason};
use novarocks::runtime::native_fragment_query::NativeFragmentQueryRuntime;
use novarocks_types::UniqueId;

use crate::ConnectorExecutionHost;
use crate::fragment::control::FragmentControlRegistry;

use super::registry::QueryLifecycleLocalRuntime;

pub(crate) struct NativeQueryLifecycleLocalRuntime {
    runtime: NativeFragmentQueryRuntime,
    controls: Arc<FragmentControlRegistry>,
    execution_host: Arc<ConnectorExecutionHost>,
}

impl NativeQueryLifecycleLocalRuntime {
    pub(crate) fn new(
        controls: Arc<FragmentControlRegistry>,
        execution_host: Arc<ConnectorExecutionHost>,
    ) -> Self {
        Self {
            runtime: NativeFragmentQueryRuntime::global(),
            controls,
            execution_host,
        }
    }
}

impl QueryLifecycleLocalRuntime for NativeQueryLifecycleLocalRuntime {
    fn terminate_query(
        &self,
        execution_id: QueryExecutionId,
        expected_instances: &[UniqueId],
        _reason: QueryTerminationReason,
        detail: &str,
    ) {
        let mut fragment_instance_ids = expected_instances.iter().copied().collect::<BTreeSet<_>>();
        fragment_instance_ids.extend(
            self.runtime
                .cancel_execution(execution_id, detail.to_string()),
        );
        let fragment_instance_ids = fragment_instance_ids.into_iter().collect::<Vec<_>>();
        self.controls.cancel_many(&fragment_instance_ids, detail);
        // Query lifecycle is the sole terminal authority for execution
        // leases. Releasing here covers Finalize, Abort, deadline, KILL QUERY
        // and BE shutdown without a second connector-specific release RPC.
        let _ = self.execution_host.release_query(execution_id);
        if fragment_failure_test_markers_enabled() {
            for finst_id in fragment_instance_ids {
                eprintln!(
                    "NOVAROCKS_CANCEL_FINST query_hi={} query_lo={} finst_hi={} finst_lo={}",
                    execution_id.query_id().high(),
                    execution_id.query_id().low(),
                    finst_id.high(),
                    finst_id.low()
                );
            }
        }
    }
}

#[cfg(debug_assertions)]
fn fragment_failure_test_markers_enabled() -> bool {
    std::env::var_os("NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE").is_some()
}

#[cfg(not(debug_assertions))]
fn fragment_failure_test_markers_enabled() -> bool {
    false
}
