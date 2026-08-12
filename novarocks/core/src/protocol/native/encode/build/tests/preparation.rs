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

use super::*;

struct SentinelDeltaResolver {
    calls: AtomicUsize,
}

impl crate::query_execution::preparation::scan::ScanBindingResolver for SentinelDeltaResolver {
    fn resolve_scan(
        &self,
        node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<crate::query_execution::preparation::scan::ResolvedScanExecution>, String>
    {
        self.calls.fetch_add(1, Ordering::Relaxed);
        assert_eq!(node_id, 10);
        assert!(matches!(
            scan.table.source,
            ScanSource::Sql(crate::sql::planner::table::SqlScanSource {
                kind: crate::sql::planner::table::SqlScanKind::Delta {
                    from_snapshot_id: 6,
                    to_snapshot_id: 7,
                    ..
                },
                ..
            })
        ));
        Ok(Some(
            crate::query_execution::preparation::scan::ResolvedScanExecution::SealedConnectorScan(
                crate::query_execution::preparation::scan::fixture_sealed_change_scan(
                    "test_catalog",
                    6,
                    7,
                ),
            ),
        ))
    }
}

#[test]
fn fragment_build_prepares_delta_once_without_mutating_input_plan() {
    let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
        iceberg_scan_plan(Some(vec!["id"])),
        Default::default(),
        |draft| {
            let DistributedNodeKind::Scan(scan) = &mut draft.fragments_mut()[0].root.payload else {
                panic!("root must be scan");
            };
            scan.table.source = crate::sql::planner::table::test_sql_scan_source(
                crate::sql::planner::table::SqlScanKind::Delta {
                    from_snapshot_id: 6,
                    to_snapshot_id: 7,
                },
            );
        },
    );
    let before = format!("{plan:#?}");
    let resolver = SentinelDeltaResolver {
        calls: AtomicUsize::new(0),
    };
    let connectors = ConnectorRegistry::new();
    crate::connector::scan_model::register_planned_files_fixture(
        &connectors,
        "test_catalog",
        Vec::new(),
        None,
    );

    let result = build_for_test(TestBuildRequest {
        distributed_plan: &plan,
        catalog: &EmptyCatalog,
        connectors: &connectors,
        scan_binding_resolver: Some(&resolver),
    })
    .expect("build prepared delta fragment");

    assert_eq!(
        resolver.calls.load(Ordering::Relaxed),
        1,
        "delta binding must resolve once"
    );
    assert_eq!(format!("{plan:#?}"), before);
    let ranges = result
        .0
        .scheduling_view()
        .scan_ranges(0, 10)
        .expect("delta ranges by original node id");
    assert!(ranges.is_empty());
    assert_eq!(
        result
            .0
            .scheduling_view()
            .connector_read(0, 10)
            .expect("delta opaque connector read")
            .splits
            .len(),
        0
    );
}

#[test]
fn fragment_build_reports_missing_delta_resolver_before_encoding() {
    let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
        iceberg_scan_plan(Some(vec!["id"])),
        Default::default(),
        |draft| {
            let DistributedNodeKind::Scan(scan) = &mut draft.fragments_mut()[0].root.payload else {
                panic!("root must be scan");
            };
            scan.table.source = crate::sql::planner::table::test_sql_scan_source(
                crate::sql::planner::table::SqlScanKind::Delta {
                    from_snapshot_id: 6,
                    to_snapshot_id: 7,
                },
            );
        },
    );

    let connectors = ConnectorRegistry::new();
    crate::connector::scan_model::register_planned_files_fixture(
        &connectors,
        "test_catalog",
        Vec::new(),
        None,
    );
    let err = match build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &connectors,
        None,
    )) {
        Ok(_) => panic!("delta scan without resolver must fail during preparation"),
        Err(err) => err,
    };

    assert!(err.contains("SqlDelta"), "{err}");
    assert!(err.contains("node_id=10"), "{err}");
    assert!(err.contains("from_snapshot_id=6"), "{err}");
    assert!(err.contains("to_snapshot_id=7"), "{err}");
    assert!(err.contains("requires scan binding resolver"), "{err}");
}
