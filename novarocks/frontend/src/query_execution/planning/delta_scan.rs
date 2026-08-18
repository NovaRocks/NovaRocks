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

//! Application-owned lookup for provider-sealed change-window scans.
//!
//! The provider admits the physical change read while the refresh owns its
//! exact planning lease. Preparation retrieves only the sealed SPI scan by its
//! query-local token and snapshot window.

use crate::query_execution::preparation::scan::{ResolvedScanExecution, ScanBindingResolver};
use novarocks::catalog_application::query_bindings::QueryTableBindingStore;
use novarocks_sql::plan_read::PlanScanNode;
use novarocks_sql::planning::query_execution::{
    SqlScanPreparationCategory, scan_preparation_facts,
};

/// Exact query-local delta lookup.  It intentionally accepts neither a
/// refresh context nor a catalog/registry, so it cannot reacquire metadata or
/// a newer connector generation after compilation.
pub(crate) struct QueryTableBindingScanResolver<'a> {
    bindings: &'a QueryTableBindingStore,
}

impl<'a> QueryTableBindingScanResolver<'a> {
    pub(crate) fn new(bindings: &'a QueryTableBindingStore) -> Self {
        Self { bindings }
    }
}

impl ScanBindingResolver for QueryTableBindingScanResolver<'_> {
    fn resolve_scan(
        &self,
        _node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        let facts = scan_preparation_facts(scan);
        if facts.category() != SqlScanPreparationCategory::Delta {
            return Ok(None);
        }
        let window = facts.delta_window().ok_or_else(|| {
            format!(
                "SQL delta scan facts for '{}' are missing a sealed change window",
                facts.identity().fqn()
            )
        })?;
        let from_snapshot_id = window.from_snapshot_id();
        let to_snapshot_id = window.to_snapshot_id();
        let binding = self.bindings.binding(facts.binding())?;
        let admitted_scan = binding
            .admitted_change_scans
            .get(&(from_snapshot_id, to_snapshot_id))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "SQL delta scan binding for '{}.{}.{}' has no sealed change-window admission from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}",
                    facts.identity().catalog(),
                    facts.identity().namespace(),
                    facts.identity().table()
                )
            })?;
        Ok(Some(ResolvedScanExecution::SealedConnectorScan(
            admitted_scan,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorChangeWindow, ConnectorChangeWindowAdmission,
        ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorRequestContext, ConnectorScan, ConnectorScanHandle,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::{QueryTableBindingScanResolver, ScanBindingResolver};
    use crate::query_execution::preparation::scan::ResolvedScanExecution;
    use novarocks::catalog_application::query_bindings::{
        QueryTableBindingKey, QueryTableBindingStore, admitted_change_window_binding_for_test,
    };
    use novarocks_sql::plan_read::{DistributedNodeKind, PlanScanNode};
    use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn admitted_scan(from_snapshot_id: i64, to_snapshot_id: i64) -> ConnectorScan {
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance ID"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request context");
        ConnectorScan::try_new_change_window(
            owner.clone(),
            ConnectorChangeWindow::new(from_snapshot_id, to_snapshot_id),
            ConnectorChangeWindowAdmission::MetadataOnly,
            ConnectorScanHandle::try_new(owner.instance_id, Bytes::from_static(b"change-v1"))
                .expect("scan handle"),
            Arc::new(Schema::empty()),
            Vec::new(),
            &context,
        )
        .expect("sealed scan")
    }

    fn delta_scan(fixture: NativeScanFixture) -> PlanScanNode {
        let plan = native_scan_plan(fixture).expect("sealed delta scan fixture");
        plan.fragments()
            .iter()
            .find_map(|fragment| match &fragment.root.payload {
                DistributedNodeKind::Scan(scan) => Some(scan.clone()),
                _ => None,
            })
            .expect("sealed delta fixture has one scan")
    }

    fn test_store() -> QueryTableBindingStore {
        QueryTableBindingStore::try_new_with_scope_for_test(
            NonZeroU64::new(1).expect("fixture binding scope"),
        )
    }

    #[test]
    fn sqlx2_preparation_delta_resolves_only_its_admitted_window() {
        let bindings = test_store();
        bindings
            .resolve_or_insert_with_id(
                QueryTableBindingKey::snapshot("test_catalog", "test_db", "test_table", 7),
                |binding| {
                    Ok(admitted_change_window_binding_for_test(
                        binding,
                        6,
                        7,
                        admitted_scan(6, 7),
                    ))
                },
            )
            .expect("admit binding");
        let resolver = QueryTableBindingScanResolver::new(&bindings);
        let scan = delta_scan(NativeScanFixture::DeltaForPreparedBinding);

        let resolved = resolver
            .resolve_scan(7, &scan)
            .expect("resolve admitted delta")
            .expect("delta scan execution");
        let ResolvedScanExecution::SealedConnectorScan(scan) = resolved else {
            panic!("expected sealed connector scan");
        };
        assert_eq!(
            scan.selection(),
            novarocks_spi::connector::ConnectorScanSelection::ChangeWindow(
                ConnectorChangeWindow::new(6, 7)
            )
        );

        let unadmitted = delta_scan(NativeScanFixture::DeltaWithStaleUnprojectedColumn);
        let error = resolver
            .resolve_scan(7, &unadmitted)
            .expect_err("unadmitted window must fail before submission");
        assert!(
            error.contains("no sealed change-window admission"),
            "error={error}"
        );
    }

    #[test]
    fn sqlx2_preparation_delta_rejects_cross_request_token() {
        let first = QueryTableBindingStore::try_new().expect("first binding store");
        let second = test_store();
        second
            .resolve_or_insert_with_id(
                QueryTableBindingKey::snapshot("test_catalog", "test_db", "test_table", 7),
                |binding| {
                    Ok(admitted_change_window_binding_for_test(
                        binding,
                        6,
                        7,
                        admitted_scan(6, 7),
                    ))
                },
            )
            .expect("admit second binding");
        let scan = delta_scan(NativeScanFixture::DeltaForPreparedBinding);

        let error = QueryTableBindingScanResolver::new(&first)
            .resolve_scan(8, &scan)
            .expect_err("cross-request token must fail before connector preparation");
        assert!(error.contains("different request"), "error={error}");
    }
}
