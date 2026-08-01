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

#[test]
fn plan_leaf_decoders_are_owned_by_the_starrocks_protocol_boundary() {
    let _ = super::expr::lower_t_expr;
    let _ = super::layout::build_tuple_slot_order;
    let _ = super::node::lower_fetch_node;
    let _ = super::descriptor::descriptor_snapshot_from_thrift;
}

#[test]
fn per_submission_iceberg_table_locations_are_isolated() {
    use std::collections::HashMap;

    use novarocks::runtime::descriptor_snapshot::{
        DescriptorSnapshot, DescriptorTable, DescriptorTableKind, IcebergTableLocationMap,
    };

    fn snapshot(location: &str) -> DescriptorSnapshot {
        DescriptorSnapshot::new_with_tables(
            Vec::new(),
            HashMap::new(),
            vec![DescriptorTable {
                id: 7,
                kind: DescriptorTableKind::Iceberg,
                location: Some(location.to_string()),
                iceberg_schema: None,
            }],
        )
        .expect("descriptor snapshot")
    }

    let first = IcebergTableLocationMap::from_snapshot(&snapshot("s3://warehouse/first"));
    let second = IcebergTableLocationMap::from_snapshot(&snapshot("s3://warehouse/second"));

    assert_eq!(first.get(7), Some("s3://warehouse/first"));
    assert_eq!(second.get(7), Some("s3://warehouse/second"));
}

#[test]
fn fragment_execution_leaf_fields_are_protocol_neutral() {
    use novarocks::connector::starrocks::scan::LakeScanSchemaMeta;
    use novarocks::exec::node::fetch::FetchNode;
    use novarocks::runtime::descriptor_snapshot::LookupNodesInfo;
    use novarocks::runtime::endpoint::RuntimeEndpoint;

    fn fetch_nodes_info(fetch: &FetchNode) -> Option<&LookupNodesInfo> {
        fetch.nodes_info.as_ref()
    }

    fn lake_schema_fe_addr(meta: &LakeScanSchemaMeta) -> Option<&RuntimeEndpoint> {
        meta.fe_addr.as_ref()
    }

    let _ = fetch_nodes_info;
    let _ = lake_schema_fe_addr;
}

#[test]
fn exchange_sender_fallback_uses_only_the_explicit_batch_map() {
    use std::collections::HashMap;

    use super::node::resolve_exchange_sender_count;

    let sender_counts = HashMap::from([(17, 3)]);
    assert_eq!(
        resolve_exchange_sender_count(17, None, &sender_counts).expect("batch sender count"),
        3
    );
    assert!(resolve_exchange_sender_count(18, None, &sender_counts).is_err());
}

#[test]
fn query_profile_decode_declares_a_stable_external_dependency() {
    use std::collections::BTreeMap;

    use super::{StarRocksExternalDependency, StarRocksExternalDependencyDraft};

    let first = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
    let second = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
    assert_eq!(first.query_profile("query-7").expect("draft value"), "");
    assert_eq!(second.query_profile("query-7").expect("draft value"), "");

    let first = first.external_dependencies();
    let second = second.external_dependencies();
    assert_eq!(first, second);
    assert!(matches!(
        first.as_slice(),
        [StarRocksExternalDependency::QueryProfile { query_id, .. }] if query_id == "query-7"
    ));
}

#[test]
fn lake_meta_decode_declares_stable_storage_facts_without_resolving_storage() {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::{StarRocksExternalDependency, StarRocksExternalDependencyDraft};
    use novarocks::connector::starrocks::lake_meta::{
        LakeMetaColumnKind, LakeMetaColumnRequest, LakeMetaStorageRequest, LakeMetaTabletRequest,
    };
    use novarocks_types::QueryId;

    fn request() -> LakeMetaStorageRequest {
        LakeMetaStorageRequest::new(
            QueryId::new(1, 2),
            "default_catalog".to_string(),
            "db".to_string(),
            "table".to_string(),
            3,
            4,
            5,
            vec![LakeMetaTabletRequest {
                tablet_id: 6,
                version: 7,
                row_count_hint: Some(8),
            }],
            vec![LakeMetaColumnRequest {
                column_id: "c1".to_string(),
                kind: LakeMetaColumnKind::Value(DataType::Int64),
            }],
        )
    }

    let first = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
    let second = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
    let first_placeholder = first.lake_meta_storage(&request()).expect("draft facts");
    let second_placeholder = second.lake_meta_storage(&request()).expect("draft facts");
    assert_eq!(first_placeholder.total_rows, 0);
    assert_eq!(second_placeholder.total_rows, 0);
    assert!(first_placeholder.column_arrays.values().all(Vec::is_empty));

    let first = first.external_dependencies();
    let second = second.external_dependencies();
    assert_eq!(first, second);
    assert!(matches!(
        first.as_slice(),
        [StarRocksExternalDependency::LakeMetaStorage { request, .. }]
            if request.table_id == 4 && request.tablets[0].tablet_id == 6
    ));
}
