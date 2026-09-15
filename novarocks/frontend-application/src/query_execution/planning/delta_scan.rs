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

//! Application-owned lookup for change-window scans.
//!
//! The statement admits the relation while it still holds its exact planning
//! lease; the refresh owns that lease for the rest of compilation. This lookup
//! recovers only the admitted materialization named by the scan's query-local
//! token, and it validates that the scan states both window endpoints.
//!
//! It deliberately does not resolve the window itself. A change window is the
//! set difference between the rows visible at its two endpoints — never a
//! replay of the manifests between them, in which a row written and deleted
//! inside the window would appear although it is invisible at both endpoints.
//! Only the connector can compute that difference, so preparation hands it the
//! two endpoints and the connector freezes the relation.

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::query_execution::preparation::scan::{ResolvedScanExecution, ScanBindingResolver};
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
        // Both endpoints must be stated by the scan. A window missing one has
        // no set difference to describe at all.
        facts.delta_window().ok_or_else(|| {
            format!(
                "SQL delta scan facts for '{}' are missing a change window",
                facts.identity().fqn()
            )
        })?;
        let materialization = self
            .bindings
            .scan_materialization(facts.binding())?
            .ok_or_else(|| {
                format!(
                    "SQL delta scan binding for '{}.{}.{}' has no scan materialization",
                    facts.identity().catalog(),
                    facts.identity().namespace(),
                    facts.identity().table()
                )
            })?;
        Ok(Some(ResolvedScanExecution::AdmittedChangeWindow(
            materialization,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use novarocks_spi::connector::{ConnectorReadSelector, ConnectorTablePlanningFacts};

    use super::{QueryTableBindingScanResolver, ScanBindingResolver};
    use crate::catalog_application::query_bindings::{
        QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission,
        QueryTableBindingKey, QueryTableBindingStore,
    };
    use crate::query_execution::preparation::scan::{
        ResolvedScanExecution, fixture_query_scan_materialization,
    };
    use novarocks_sql::binding::SqlTableBindingId;
    use novarocks_sql::plan_read::{DistributedNodeKind, PlanScanNode};
    use novarocks_sql::planning::catalog::{
        ConnectorReadTableFacts, materialize_connector_read_table,
    };
    use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};

    /// One admitted binding of the delta fixture's table, with or without the
    /// scan materialization the change-window lane must recover.
    fn change_window_binding(
        binding: SqlTableBindingId,
        materialization: Option<QueryScanMaterialization>,
    ) -> QueryTableBinding {
        let resolved = materialize_connector_read_table(ConnectorReadTableFacts {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            schema: Arc::new(Schema::empty()),
            binding,
            selector: ConnectorReadSelector::Current,
            planning_facts: ConnectorTablePlanningFacts::empty(),
        })
        .expect("test catalog facts materialize")
        .into_resolved_table();
        QueryTableBinding {
            resolved,
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Local,
            scan_materialization: materialization,
            mv_target_read: None,
            write_target_admission: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        }
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

    fn admit(store: &QueryTableBindingStore, materialization: Option<QueryScanMaterialization>) {
        store
            .resolve_or_insert_with_id(
                QueryTableBindingKey::snapshot("test_catalog", "test_db", "test_table", 7),
                |binding| Ok(change_window_binding(binding, materialization.clone())),
            )
            .expect("admit binding");
    }

    /// The delta lane recovers the exact query-local admission of its relation.
    /// The window itself stays on the scan: the connector is what turns two
    /// endpoints into one change-window relation.
    #[test]
    fn a_delta_scan_resolves_its_exact_query_local_admission() {
        let bindings = test_store();
        let materialization = fixture_query_scan_materialization("test_catalog");
        admit(&bindings, Some(materialization.clone()));
        let resolver = QueryTableBindingScanResolver::new(&bindings);
        let scan = delta_scan(NativeScanFixture::DeltaForPreparedBinding);

        let resolved = resolver
            .resolve_scan(7, &scan)
            .expect("resolve admitted delta")
            .expect("delta scan execution");
        let ResolvedScanExecution::AdmittedChangeWindow(resolved) = resolved else {
            panic!("expected an admitted change window");
        };
        assert_eq!(
            resolved.planning_lease.binding().incarnation(),
            materialization.planning_lease.binding().incarnation(),
            "the lane must reuse the exact admitted generation"
        );
    }

    /// A binding with no admitted materialization is a submission-time
    /// contract failure, never permission to resolve the relation again.
    #[test]
    fn a_delta_scan_without_an_admitted_materialization_fails_closed() {
        let bindings = test_store();
        admit(&bindings, None);
        let error = QueryTableBindingScanResolver::new(&bindings)
            .resolve_scan(7, &delta_scan(NativeScanFixture::DeltaForPreparedBinding))
            .expect_err("an unadmitted relation must fail before submission");
        assert!(error.contains("no scan materialization"), "error={error}");
    }

    #[test]
    fn sqlx2_preparation_delta_rejects_cross_request_token() {
        // A named scope keeps this a cross-request test whatever order the
        // suite runs in: an allocated scope could coincide with the fixture's
        // and turn the refusal into "missing from this request" instead.
        let first = QueryTableBindingStore::try_new_with_scope_for_test(
            NonZeroU64::new(2).expect("second fixture binding scope"),
        );
        let second = test_store();
        admit(
            &second,
            Some(fixture_query_scan_materialization("test_catalog")),
        );
        let scan = delta_scan(NativeScanFixture::DeltaForPreparedBinding);

        let error = QueryTableBindingScanResolver::new(&first)
            .resolve_scan(8, &scan)
            .expect_err("cross-request token must fail before connector preparation");
        assert!(error.contains("different request"), "error={error}");
    }
}
