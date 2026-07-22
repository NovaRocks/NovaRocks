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

use std::collections::BTreeMap;

use arrow::datatypes::DataType;

use super::*;
use crate::connector::iceberg::scan_model as iceberg_scan_model;
use crate::connector::scan_model::starrocks as connector_scan;
use crate::coordinator::prepare::PreparedFragmentSet;
use crate::coordinator::prepare::scan::IcebergDeltaScanRuntimePlan;
use crate::coordinator::prepare::scan::{
    ResolvedIcebergDeltaScan, ResolvedIcebergFileScan, ResolvedReadColumn, ResolvedReadReason,
    ResolvedScanBinding, ResolvedScanColumn, ResolvedScanColumnKind, ResolvedScanExecution,
    ScanExecutionBindings,
};
use crate::protocol::native::encode::plan as native_plan;
use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::DataPartition;
use crate::sql::planner::table as table_model;

fn prepared_runtime_filter_bindings(plan: &DistributedPlan) -> &'static PreparedFragmentSet {
    Box::leak(Box::new(
        crate::coordinator::prepare::prepared_fragment_set_for_native_encode_test(plan)
            .expect("materialize native encoder test binding tables"),
    ))
}

#[test]
fn iceberg_delta_table_encoder_consumes_prepared_binding_payload() {
    let plan = iceberg_delta_distributed_plan_for_test();
    let source_column = novarocks_catalog::schema::ColumnDef {
        name: "physical_order_id".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        write_default: None,
        logical_type: None,
    };
    let mut plan = crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
    root_scan_for_test(&mut plan)
        .table
        .columns
        .push(column_def_for_test(
            "stale_unprojected",
            DataType::Utf8,
            true,
        ));
    let plan = plan.seal().expect("seal prepared delta fixture");
    let hidden_equality_column = column_def_for_test("tenant_id", DataType::Int64, false);
    let mut bindings = ScanExecutionBindings::default();
    bindings
        .insert_binding(ResolvedScanBinding {
            node_id: 10,
            execution: ResolvedScanExecution::IcebergDelta(ResolvedIcebergDeltaScan {
                runtime_plan: IcebergDeltaScanRuntimePlan {
                    table_location: "s3://prepared/orders".to_string(),
                    data_columns: Vec::new(),
                    cloud_properties: BTreeMap::from([(
                        "endpoint".to_string(),
                        "http://prepared-minio".to_string(),
                    )]),
                    change_files: Vec::new(),
                    delete_side: None,
                },
            }),
            physical_columns: vec![ResolvedScanColumn {
                planner: output_column(1, "bound_order_id", DataType::Int64),
                source: source_column.clone(),
                kind: ResolvedScanColumnKind::PhysicalTableColumn,
            }],
            required_reads: vec![
                ResolvedReadColumn {
                    planner_column_id: Some(ColumnId::new_for_test(1)),
                    source: source_column,
                    reason: ResolvedReadReason::PlannerRequiredOrOutput,
                },
                ResolvedReadColumn {
                    planner_column_id: None,
                    source: hidden_equality_column,
                    reason: ResolvedReadReason::EqualityDeleteKey,
                },
            ],
        })
        .expect("insert prepared delta binding");

    let encoded = native_plan::encode_distributed_plan_with_context(
        &plan,
        native_plan::NativePlanEncodeContext {
            scan_bindings: Some(&bindings),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
        },
    )
    .expect("encode prepared delta binding");

    let root = encoded.fragments[0].root.as_ref().expect("encoded root");
    let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
        root.payload.as_ref()
    else {
        panic!("expected physical root");
    };
    let Some(crate::proto::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
        panic!("expected scan root");
    };
    assert_eq!(scan.columns[0].name, "physical_order_id");
    assert_eq!(
        scan.required_columns,
        vec!["physical_order_id", "tenant_id"]
    );
    let table = scan.table.as_ref().expect("bound table");
    assert_eq!(
        table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["physical_order_id", "tenant_id"]
    );
    assert!(table.iceberg_row_lineage_metadata_columns.is_empty());
    let Some(crate::proto::plan::scan_source::Kind::IcebergDeltaTable(delta)) = table
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
    else {
        panic!("expected encoded delta source");
    };
    let runtime = delta.delta_plan.as_ref().expect("prepared runtime payload");
    assert_eq!(runtime.table_location, "s3://prepared/orders");
    assert_eq!(
        runtime.cloud_properties.get("endpoint").map(String::as_str),
        Some("http://prepared-minio")
    );
}

#[test]
fn ordinary_iceberg_binding_preserves_existing_encoding() {
    let plan = iceberg_delta_distributed_plan_for_test();
    let mut plan = crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
    let scan = root_scan_for_test(&mut plan);
    scan.table.columns.push(column_def_for_test(
        "unprojected_payload",
        DataType::Utf8,
        true,
    ));
    let table = iceberg_table_info_for_test();
    scan.table.source = table_model::ScanSource::IcebergDataFiles {
        table: table.clone(),
        files: Vec::new(),
        cloud_properties: BTreeMap::from([("region".to_string(), "test".to_string())]),
        binding: iceberg_scan_model::IcebergDataFileBinding::CurrentSnapshot,
    };
    scan.required_columns = Some(vec!["order_id".to_string()]);
    let plan = plan.seal().expect("seal ordinary Iceberg fixture");

    let without_binding = encode_distributed_plan(&plan, empty_scan_bindings())
        .expect("encode ordinary Iceberg scan");
    let mut bindings = ScanExecutionBindings::default();
    bindings
        .insert_binding(file_binding_for_test(
            10,
            table,
            iceberg_scan_model::IcebergDataFileBinding::CurrentSnapshot,
            vec![bound_column_for_test(
                1,
                "order_id",
                "order_id",
                ResolvedScanColumnKind::PhysicalTableColumn,
            )],
            vec![bound_read_for_test(Some(1), "order_id")],
        ))
        .expect("insert ordinary Iceberg binding");
    let with_binding = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&bindings),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
        },
    )
    .expect("encode ordinary Iceberg binding");

    assert_eq!(with_binding, without_binding);
}

#[test]
fn refresh_file_bindings_drive_source_projection_metadata_and_hidden_reads() {
    let refresh_sources = [
        table_model::ScanSource::IcebergVersionTable {
            table: iceberg_table_info_for_test(),
            snapshot_id: 1,
        },
        table_model::ScanSource::IcebergMvTargetLocator(table_model::IcebergMvTargetLocatorScan {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            table: "orders".to_string(),
            target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
            target_snapshot_id: Some(1),
            apply_key_column: "bound_order_id".to_string(),
            branch_id_column: None,
        }),
        table_model::ScanSource::IcebergMvTargetState(table_model::IcebergMvTargetStateScan {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            table: "orders".to_string(),
            target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
            target_snapshot_id: Some(1),
            aggregate_state_layout_version: 1,
            columns: Vec::new(),
            group_key_names: vec!["bound_order_id".to_string()],
            aggregate_state_names: Vec::new(),
            physical_column_names: vec!["bound_order_id".to_string()],
            row_id_column_name: "bound_order_id".to_string(),
            row_filter: table_model::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "bound_order_id".to_string(),
                branch_scope: None,
            },
            partition_constraint:
                table_model::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        }),
    ];

    for source in refresh_sources {
        let plan = iceberg_delta_distributed_plan_for_test();
        let mut plan =
            crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
        let scan = root_scan_for_test(&mut plan);
        scan.table.source = source;
        scan.table.columns = vec![
            column_def_for_test("stale", DataType::Utf8, true),
            column_def_for_test("stale_unprojected", DataType::Utf8, true),
        ];
        scan.columns = vec![
            output_column(1, "stale", DataType::Utf8),
            output_column(2, "stale_meta", DataType::Int64),
        ];
        let plan = plan.seal().expect("seal refresh-source fixture");

        let mut resolved_table = iceberg_table_info_for_test();
        resolved_table.current_snapshot_id = Some(1);
        resolved_table.location = "s3://resolved/orders".to_string();
        resolved_table.schema.fields[0].name = "physical_order_id".to_string();
        resolved_table
            .schema
            .fields
            .push(iceberg_scan_model::IcebergSchemaFieldDef {
                field_id: 2,
                name: "tenant_id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            });
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(file_binding_for_test(
                10,
                resolved_table,
                iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles,
                vec![
                    ResolvedScanColumn {
                        planner: output_column(1, "bound_order_id", DataType::Int64),
                        source: column_def_for_test("physical_order_id", DataType::Int64, false),
                        kind: ResolvedScanColumnKind::PhysicalTableColumn,
                    },
                    ResolvedScanColumn {
                        planner: output_column(2, "bound_file", DataType::Utf8),
                        source: column_def_for_test("_file", DataType::Utf8, false),
                        kind: ResolvedScanColumnKind::IcebergMetadataColumn,
                    },
                ],
                vec![
                    bound_read_for_test(Some(1), "physical_order_id"),
                    ResolvedReadColumn {
                        planner_column_id: None,
                        source: column_def_for_test("tenant_id", DataType::Int64, false),
                        reason: ResolvedReadReason::EqualityDeleteKey,
                    },
                ],
            ))
            .expect("insert refresh file binding");

        let encoded = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&bindings),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
            },
        )
        .expect("encode refresh binding");
        let scan = encoded_root_scan_for_test(&encoded);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["physical_order_id", "_file"]
        );
        assert_eq!(
            scan.required_columns,
            vec!["physical_order_id", "tenant_id"]
        );
        let table = scan.table.as_ref().expect("bound table");
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["physical_order_id", "tenant_id"],
            "resolver-required sources must encode only binding-owned physical columns and hidden reads"
        );
        assert_eq!(
            table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["_file"]
        );
        let Some(crate::proto::plan::scan_source::Kind::IcebergDataFiles(files)) = table
            .source
            .as_ref()
            .and_then(|source| source.kind.as_ref())
        else {
            panic!("refresh source must encode as resolved IcebergDataFiles");
        };
        assert_eq!(
            files.table.as_ref().expect("resolved table").location,
            "s3://resolved/orders"
        );
        assert_eq!(
            files.binding,
            crate::proto::plan::IcebergDataFileBinding::ExplicitFiles as i32
        );
        let (read_columns, variants) = crate::protocol::native::decode::scan_read_binding_for_test(
            scan,
            files.table.as_ref().expect("resolved table"),
            &scan.columns,
        )
        .expect("lower bound refresh read plan");
        assert!(
            read_columns.iter().any(|column| column == "tenant_id"),
            "native lowering must resolve hidden equality key from TableDef"
        );
        assert!(variants.is_empty());
    }
}

#[test]
fn required_bindings_reject_missing_node_and_execution_variant_mismatch() {
    let plan = iceberg_delta_distributed_plan_for_test();
    let missing = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&ScanExecutionBindings::default()),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
        },
    )
    .expect_err("delta source without prepared binding must fail");
    assert!(missing.contains("node_id=10"), "{missing}");
    assert!(missing.contains("IcebergDeltaTable"), "{missing}");
    assert!(missing.contains("from_snapshot_id=1"), "{missing}");
    assert!(missing.contains("to_snapshot_id=2"), "{missing}");

    let mut wrong_node = ScanExecutionBindings::default();
    wrong_node
        .insert_binding(delta_binding_for_test(11))
        .expect("insert binding for wrong node");
    let err = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&wrong_node),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
        },
    )
    .expect_err("binding at another node id must not be reused");
    assert!(err.contains("node_id=10"), "{err}");

    let mut wrong_execution = ScanExecutionBindings::default();
    wrong_execution
        .insert_binding(file_binding_for_test(
            10,
            iceberg_table_info_for_test(),
            iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles,
            vec![bound_column_for_test(
                1,
                "order_id",
                "order_id",
                ResolvedScanColumnKind::PhysicalTableColumn,
            )],
            vec![bound_read_for_test(Some(1), "order_id")],
        ))
        .expect("insert wrong execution variant");
    let err = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&wrong_execution),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
        },
    )
    .expect_err("delta source with file binding must fail");
    assert!(err.contains("execution variant mismatch"), "{err}");
    assert!(err.contains("IcebergFiles"), "{err}");
}

#[test]
fn binding_encoder_preserves_variant_synthetic_output_and_required_name() {
    let plan = iceberg_delta_distributed_plan_for_test();
    let mut plan = crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
    let scan = root_scan_for_test(&mut plan);
    let mut table = iceberg_table_info_for_test();
    table.schema.fields[0].name = "v".to_string();
    scan.table.columns = vec![column_def_for_test("v", DataType::LargeBinary, false)];
    scan.table.source = table_model::ScanSource::IcebergDataFiles {
        table: table.clone(),
        files: Vec::new(),
        cloud_properties: BTreeMap::new(),
        binding: iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles,
    };
    scan.columns = vec![
        output_column(1, "v", DataType::LargeBinary),
        OutputColumn {
            column_id: ColumnId::new_for_test(2),
            name: "__nr_var_v_0".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: true,
        },
    ];
    scan.required_columns = Some(vec!["__nr_var_v_0".to_string()]);
    scan.variant_columns = vec![crate::sql::common::ScanVariantColumn {
        source_column_id: ColumnId::new_for_test(1),
        source_column: "v".to_string(),
        synthetic_column_id: ColumnId::new_for_test(2),
        synthetic_column: "__nr_var_v_0".to_string(),
        canonical_path: "$.a.b".to_string(),
        requested_type: DataType::Int64,
        strict: true,
    }];
    let plan = plan.seal().expect("seal variant fixture");
    let mut bindings = ScanExecutionBindings::default();
    bindings
        .insert_binding(file_binding_for_test(
            10,
            table,
            iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles,
            vec![ResolvedScanColumn {
                planner: output_column(1, "v", DataType::LargeBinary),
                source: column_def_for_test("v", DataType::LargeBinary, false),
                kind: ResolvedScanColumnKind::PhysicalTableColumn,
            }],
            Vec::new(),
        ))
        .expect("insert variant binding");

    let encoded = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&bindings),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
        },
    )
    .expect("encode bound VARIANT scan");
    let scan = encoded_root_scan_for_test(&encoded);
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "v"), (2, "__nr_var_v_0")]
    );
    assert_eq!(scan.required_columns, vec!["__nr_var_v_0"]);
    assert_eq!(scan.variant_columns[0].synthetic_column_id, 2);
    let table = scan.table.as_ref().expect("bound table");
    let Some(crate::proto::plan::scan_source::Kind::IcebergDataFiles(files)) = table
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
    else {
        panic!("variant binding must encode as IcebergDataFiles");
    };
    let (read_columns, variants) = crate::protocol::native::decode::scan_read_binding_for_test(
        scan,
        files.table.as_ref().expect("resolved table"),
        &scan.columns[1..],
    )
    .expect("lower encoded bound VARIANT scan");
    assert_eq!(read_columns, vec!["v"]);
    assert_eq!(variants, vec![(1, 2)]);
}

fn root_scan_for_test(
    plan: &mut crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder,
) -> &mut crate::sql::planner::payload::PlanScanNode {
    let DistributedNodeKind::Scan(scan) = &mut plan.fragments_mut()[0].root.payload else {
        panic!("expected root scan");
    };
    scan
}

fn encoded_root_scan_for_test(plan: &plan::DistributedPlan) -> &plan::ScanNode {
    let root = plan.fragments[0].root.as_ref().expect("encoded root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical root");
    };
    let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
        panic!("expected scan root");
    };
    scan
}

fn file_binding_for_test(
    node_id: i32,
    table: iceberg_scan_model::IcebergTableInfo,
    file_binding: iceberg_scan_model::IcebergDataFileBinding,
    physical_columns: Vec<ResolvedScanColumn>,
    required_reads: Vec<ResolvedReadColumn>,
) -> ResolvedScanBinding {
    ResolvedScanBinding {
        node_id,
        execution: ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
            table,
            files: Vec::new(),
            cloud_properties: BTreeMap::from([("region".to_string(), "test".to_string())]),
            binding: file_binding,
        }),
        physical_columns,
        required_reads,
    }
}

fn delta_binding_for_test(node_id: i32) -> ResolvedScanBinding {
    ResolvedScanBinding {
        node_id,
        execution: ResolvedScanExecution::IcebergDelta(ResolvedIcebergDeltaScan {
            runtime_plan: IcebergDeltaScanRuntimePlan {
                table_location: "s3://prepared/orders".to_string(),
                data_columns: Vec::new(),
                cloud_properties: BTreeMap::new(),
                change_files: Vec::new(),
                delete_side: None,
            },
        }),
        physical_columns: vec![bound_column_for_test(
            1,
            "order_id",
            "order_id",
            ResolvedScanColumnKind::PhysicalTableColumn,
        )],
        required_reads: vec![bound_read_for_test(Some(1), "order_id")],
    }
}

fn bound_column_for_test(
    id: u32,
    planner_name: &str,
    source_name: &str,
    kind: ResolvedScanColumnKind,
) -> ResolvedScanColumn {
    ResolvedScanColumn {
        planner: output_column(id, planner_name, DataType::Int64),
        source: column_def_for_test(source_name, DataType::Int64, false),
        kind,
    }
}

fn bound_read_for_test(planner_id: Option<u32>, source_name: &str) -> ResolvedReadColumn {
    ResolvedReadColumn {
        planner_column_id: planner_id.map(ColumnId::new_for_test),
        source: column_def_for_test(source_name, DataType::Int64, false),
        reason: if planner_id.is_some() {
            ResolvedReadReason::PlannerRequiredOrOutput
        } else {
            ResolvedReadReason::EqualityDeleteKey
        },
    }
}

fn column_def_for_test(
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> novarocks_catalog::schema::ColumnDef {
    novarocks_catalog::schema::ColumnDef {
        name: name.to_string(),
        data_type,
        nullable,
        write_default: None,
        logical_type: None,
    }
}

fn iceberg_delta_distributed_plan_for_test() -> DistributedPlan {
    let output_columns = vec![output_column(1, "order_id", DataType::Int64)];
    crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: DistributedNode {
                node_id: 10,
                fragment_id: 0,
                tuple_ids: vec![10],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
        runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Scan(
                    crate::sql::planner::payload::PlanScanNode {
                        database: "db".to_string(),
                        table: iceberg_delta_table_for_test(),
                        alias: None,
                        columns: output_columns.clone(),
                        predicates: Vec::new(),
                        required_columns: None,
                        variant_columns: Vec::new(),
                        mv_rewritten_from: None,
                    },
                ),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: RuntimeFilterGraph::default(),
        edges: Vec::new(),
    }
}

fn iceberg_delta_table_for_test() -> table_model::TableDef {
    table_model::TableDef {
        name: "orders".to_string(),
        columns: vec![novarocks_catalog::schema::ColumnDef {
            name: "order_id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }],
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: table_model::ScanSource::IcebergDeltaTable {
            table: iceberg_table_info_for_test(),
            from_snapshot_id: 1,
            to_snapshot_id: 2,
        },
    }
}

fn iceberg_table_info_for_test() -> iceberg_scan_model::IcebergTableInfo {
    iceberg_scan_model::IcebergTableInfo {
        catalog: "ice".to_string(),
        namespace: "db".to_string(),
        table: "orders".to_string(),
        table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
        current_snapshot_id: Some(2),
        schema_id: 1,
        location: "file:///warehouse/orders".to_string(),
        schema: iceberg_scan_model::IcebergSchemaDef {
            fields: vec![iceberg_scan_model::IcebergSchemaFieldDef {
                field_id: 1,
                name: "order_id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            }],
        },
        serialized_metadata: None,
        serialized_metadata_rows: None,
    }
}

#[test]
fn native_plan_encoder_maps_starrocks_scan_source_descriptor() {
    let scan = crate::sql::planner::distributed::DistributedNode {
        node_id: 7,
        fragment_id: 0,
        tuple_ids: Vec::new(),
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Scan(
            crate::sql::planner::payload::PlanScanNode {
                database: "db".to_string(),
                table: crate::sql::planner::table::TableDef {
                    name: "sr_table".to_string(),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::sql::planner::table::ScanSource::StarRocks {
                        db_id: 1,
                        table_id: 2,
                    },
                },
                alias: None,
                columns: vec![output_column(10, "id", DataType::Int64)],
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            },
        ),
    };

    let mut bindings = ScanExecutionBindings::default();
    bindings
        .insert_starrocks_source(
            7,
            connector_scan::StarRocksScanSourceDescriptor {
                catalog_name: "default_catalog".to_string(),
                db_id: 1,
                table_id: 2,
                schema_id: 3,
                storage_columns: vec![connector_scan::StarRocksStorageColumnDescriptor {
                    name: "id".to_string(),
                    unique_id: 11,
                    default_value: Some("42".to_string()),
                }],
                tablet_schema: connector_scan::test_starrocks_tablet_schema_descriptor_for_column(
                    3,
                    "id",
                    11,
                    Some("42"),
                ),
            },
        )
        .expect("insert StarRocks source descriptor");

    let encoded = native_plan::encode_node_with_context(
        &scan,
        &native_plan::NativePlanEncodeContext {
            scan_bindings: Some(&bindings),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: None,
        },
    )
    .expect("encode StarRocks native scan source");
    let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
        encoded.payload.as_ref()
    else {
        panic!("expected physical node");
    };
    let Some(crate::proto::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
        panic!("expected scan node");
    };
    let source = scan
        .table
        .as_ref()
        .and_then(|table| table.source.as_ref())
        .and_then(|source| source.kind.as_ref())
        .expect("StarRocks source");
    let crate::proto::plan::scan_source::Kind::StarrocksTable(source) = source else {
        panic!("expected StarRocks table source, got {source:?}");
    };
    assert_eq!(source.catalog_name, "default_catalog");
    assert_eq!((source.db_id, source.table_id, source.schema_id), (1, 2, 3));
    assert_eq!(source.storage_columns.len(), 1);
    assert_eq!(source.storage_columns[0].name, "id");
    assert_eq!(source.storage_columns[0].unique_id, 11);
    assert_eq!(
        source.storage_columns[0].default_value.as_deref(),
        Some("42")
    );
    let current_schema = source
        .current_schema
        .as_ref()
        .expect("full current tablet schema");
    assert_eq!(current_schema.schema_id, 3);
    assert_eq!(
        current_schema.keys_type,
        crate::proto::plan::StarRocksKeysType::StarrocksKeysTypeDuplicate as i32
    );
    assert_eq!(current_schema.sort_key_idxes, vec![0]);
    assert_eq!(current_schema.sort_key_unique_ids, vec![11]);
    assert_eq!(current_schema.columns.len(), 1);
    assert_eq!(current_schema.columns[0].name.as_deref(), Some("id"));
    assert_eq!(current_schema.columns[0].physical_type, "BIGINT");
    assert_eq!(current_schema.columns[0].is_key, Some(true));
    assert_eq!(current_schema.columns[0].nullable, Some(true));
    assert_eq!(current_schema.columns[0].visible, Some(true));
    assert_eq!(
        current_schema.columns[0].default_value.as_deref(),
        Some("42")
    );
}

#[test]
fn native_plan_encoder_rejects_missing_starrocks_descriptor() {
    let scan = crate::sql::planner::distributed::DistributedNode {
        node_id: 7,
        fragment_id: 0,
        tuple_ids: Vec::new(),
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Scan(
            crate::sql::planner::payload::PlanScanNode {
                database: "db".to_string(),
                table: crate::sql::planner::table::TableDef {
                    name: "sr_table".to_string(),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::sql::planner::table::ScanSource::StarRocks {
                        db_id: 1,
                        table_id: 2,
                    },
                },
                alias: None,
                columns: Vec::new(),
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            },
        ),
    };

    let err = native_plan::encode_node(&scan)
        .expect_err("StarRocks scan without a builder descriptor must fail");
    assert!(err.contains("missing native source descriptor"), "{err}");
}
