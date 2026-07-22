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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::super::scan::encode_column_def;
use super::super::write::{
    encode_iceberg_change_stream_router_sink, encode_iceberg_write_sink_spec,
};
use super::*;
use crate::protocol::native::encode::plan;
use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::planner::distributed::write::change_stream::{
    IcebergChangeStreamBranchRoute, IcebergChangeStreamRouterSink,
};
use novarocks_catalog::schema::{ColumnDef, ColumnDefault};

fn encode_write_default_json_for_test(
    data_type: DataType,
    value: ColumnDefault,
) -> Result<Option<String>, String> {
    encode_column_def(&ColumnDef {
        name: "defaulted".to_string(),
        data_type,
        nullable: true,
        write_default: Some(value),
        logical_type: None,
    })
    .map(|column| column.write_default_json)
}

fn field_with_iceberg_id(id: i32, name: &str, data_type: DataType, nullable: bool) -> Arc<Field> {
    Arc::new(
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )])),
    )
}

#[test]
fn change_stream_router_encoder_materializes_partition_exprs() {
    let plan = single_fragment_router_plan_for_test();
    let fragment = plan.fragments().first().expect("router fragment");
    let DataSink::IcebergChangeStreamRouter(sink) = &fragment.sink else {
        panic!("expected Iceberg change-stream router sink");
    };
    let router = encode_iceberg_change_stream_router_sink(
        sink,
        fragment.fragment_id,
        &NativePlanEncodeContext {
            scan_bindings: None,
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: Some(plan.write_contracts()),
            runtime_filter_bindings: None,
        },
    )
    .expect("encode change-stream router sink");

    let branch = router.branches.first().expect("router branch");
    assert_eq!(branch.output_partition_ordinals, vec![2]);
    let partition = branch
        .output_partition
        .as_ref()
        .expect("branch output partition");
    assert_eq!(
        partition.kind,
        crate::proto::plan::PartitionKind::Hash as i32
    );
    let [expr] = partition.exprs.as_slice() else {
        panic!("expected one materialized partition expr");
    };
    let Some(crate::proto::expr::expr::Kind::ColumnRef(column_ref)) = expr.kind.as_ref() else {
        panic!("expected partition expr to be a column ref");
    };
    assert_eq!(column_ref.column_id, 3);
}

fn single_fragment_router_plan_for_test() -> DistributedPlan {
    let output_columns = vec![
        output_column(1, "op", DataType::Int32),
        output_column(2, "route", DataType::Int32),
        output_column(3, "bucket", DataType::Int32),
    ];
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
                payload: DistributedNodeKind::Values(
                    crate::sql::planner::payload::PlanValuesNode {
                        rows: Vec::new(),
                        columns: output_columns.clone(),
                    },
                ),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::IcebergChangeStreamRouter(IcebergChangeStreamRouterSink {
                group_id: 0,
                change_op_output_ordinal: 0,
                data_route_output_ordinal: Some(1),
                branches: vec![IcebergChangeStreamBranchRoute {
                    branch_id: 0,
                    branch_kind: ChangeStreamBranchKind::DeleteDv,
                    target_fragment_id: 1,
                    target_exchange_node_id: 20,
                    output_ordinals: vec![2],
                    output_partition_ordinals: vec![2],
                }],
            }),
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

#[test]
fn column_write_default_json_preserves_primitive_and_temporal_lexical_bytes() {
    let cases = [
        (
            "boolean",
            DataType::Boolean,
            ColumnDefault::Boolean(true),
            "true",
        ),
        ("integer", DataType::Int32, ColumnDefault::Int32(-7), "-7"),
        (
            "decimal",
            DataType::Decimal128(10, 2),
            ColumnDefault::Decimal {
                unscaled: 999,
                precision: 10,
                scale: 2,
            },
            "\"9.99\"",
        ),
        (
            "date",
            DataType::Date32,
            ColumnDefault::Date {
                days_since_epoch: 0,
            },
            "\"1970-01-01\"",
        ),
        (
            "time",
            DataType::Time64(TimeUnit::Microsecond),
            ColumnDefault::TimeMicros {
                micros_since_midnight: 0,
            },
            "\"00:00:00\"",
        ),
        (
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            ColumnDefault::TimestampMicros {
                micros_since_epoch: 1_234_567,
            },
            "\"1970-01-01T00:00:01.234567\"",
        ),
        (
            "timestamptz-normalized",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ColumnDefault::TimestamptzMicros {
                micros_since_epoch: 1_234_567,
            },
            "\"1970-01-01T00:00:01.234567\"",
        ),
        (
            "timestamp-ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            ColumnDefault::TimestampNanos {
                nanos_since_epoch: 1_234_567_890,
            },
            "\"1970-01-01T00:00:01.234567890\"",
        ),
        (
            "binary",
            DataType::Binary,
            ColumnDefault::Binary(vec![0x00, 0x0f, 0x10, 0xff]),
            "\"0f10ff\"",
        ),
    ];

    for (name, data_type, literal, expected) in cases {
        assert_eq!(
            encode_write_default_json_for_test(data_type, literal)
                .unwrap_or_else(|error| panic!("encode {name} write default: {error}"))
                .as_deref(),
            Some(expected),
            "case={name}"
        );
    }
}

#[test]
fn column_write_default_json_preserves_empty_and_nested_collection_lexical_bytes() {
    let empty_list_type =
        DataType::List(field_with_iceberg_id(1, "element", DataType::Int32, true));
    assert_eq!(
        encode_write_default_json_for_test(empty_list_type, ColumnDefault::Array(Vec::new()),)
            .unwrap()
            .as_deref(),
        Some("[]")
    );

    let empty_map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    field_with_iceberg_id(2, "key", DataType::Utf8, false),
                    field_with_iceberg_id(3, "value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    assert_eq!(
        encode_write_default_json_for_test(empty_map_type, ColumnDefault::Map(Vec::new()),)
            .unwrap()
            .as_deref(),
        Some(r#"{"keys":[],"values":[]}"#)
    );

    let list_type = DataType::List(field_with_iceberg_id(11, "element", DataType::Int32, true));
    let map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    field_with_iceberg_id(13, "key", DataType::Utf8, false),
                    field_with_iceberg_id(14, "value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let nested_type = DataType::Struct(
        vec![
            field_with_iceberg_id(10, "items", list_type, true),
            field_with_iceberg_id(12, "attributes", map_type, true),
        ]
        .into(),
    );
    let nested_literal = ColumnDefault::Struct(vec![
        (
            "items".to_string(),
            ColumnDefault::Array(vec![ColumnDefault::Int32(1), ColumnDefault::Null]),
        ),
        (
            "attributes".to_string(),
            ColumnDefault::Map(vec![
                (
                    ColumnDefault::String("first".to_string()),
                    ColumnDefault::Int32(2),
                ),
                (
                    ColumnDefault::String("second".to_string()),
                    ColumnDefault::Null,
                ),
            ]),
        ),
    ]);
    assert_eq!(
        encode_write_default_json_for_test(nested_type, nested_literal)
            .unwrap()
            .as_deref(),
        Some(r#"{"10":[1,null],"12":{"keys":["first","second"],"values":[2,null]}}"#)
    );
}

#[test]
fn column_write_default_json_preserves_non_finite_as_legacy_null() {
    let cases = [
        (
            "float-nan",
            DataType::Float32,
            ColumnDefault::Float32 { bits: 0x7fc0_1234 },
        ),
        (
            "float-positive-infinity",
            DataType::Float32,
            ColumnDefault::Float32 {
                bits: f32::INFINITY.to_bits(),
            },
        ),
        (
            "float-negative-infinity",
            DataType::Float32,
            ColumnDefault::Float32 {
                bits: f32::NEG_INFINITY.to_bits(),
            },
        ),
        (
            "double-nan",
            DataType::Float64,
            ColumnDefault::Float64 {
                bits: 0x7ff8_0000_0000_1234,
            },
        ),
        (
            "double-positive-infinity",
            DataType::Float64,
            ColumnDefault::Float64 {
                bits: f64::INFINITY.to_bits(),
            },
        ),
        (
            "double-negative-infinity",
            DataType::Float64,
            ColumnDefault::Float64 {
                bits: f64::NEG_INFINITY.to_bits(),
            },
        ),
    ];

    for (name, data_type, literal) in cases {
        assert_eq!(
            encode_write_default_json_for_test(data_type, literal)
                .unwrap_or_else(|error| panic!("encode {name} write default: {error}"))
                .as_deref(),
            Some("null"),
            "case={name}"
        );
    }
}

#[test]
fn column_write_default_json_preserves_uuid_and_fixed_unsupported_errors() {
    let cases = [
        (
            DataType::FixedSizeBinary(16),
            ColumnDefault::Uuid(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff_u128.to_be_bytes()),
            "native plan cannot encode write_default_json for Arrow type FixedSizeBinary(16) without a logical Iceberg type",
        ),
        (
            DataType::FixedSizeBinary(4),
            ColumnDefault::Fixed {
                size: 4,
                bytes: vec![0x00, 0x7f, 0x80, 0xff],
            },
            "Arrow-to-native TypeDesc conversion does not support data type FixedSizeBinary(4)",
        ),
    ];

    for (data_type, literal, expected) in cases {
        assert_eq!(
            encode_write_default_json_for_test(data_type, literal).unwrap_err(),
            expected
        );
    }
}
fn values_distributed_node(
    fragment_id: crate::sql::planner::distributed::FragmentId,
    node_id: i32,
    output: Vec<crate::sql::analysis::OutputColumn>,
) -> crate::sql::planner::distributed::DistributedNode {
    crate::sql::planner::distributed::DistributedNode {
        node_id,
        fragment_id,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Values(
            crate::sql::planner::payload::PlanValuesNode {
                rows: Vec::new(),
                columns: output,
            },
        ),
    }
}

#[test]
fn iceberg_write_fragment_uses_sink_output_contract_for_duplicate_input_columns() {
    let mut sink_spec =
        crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
    sink_spec.target_columns = vec![
        novarocks_catalog::schema::ColumnDef {
            name: "c0".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "c1".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ];
    sink_spec.target_table.columns = sink_spec.target_columns.clone();

    let repeated_input = vec![
        output_column(7, "g0", DataType::Int64),
        output_column(7, "g1", DataType::Int64),
    ];
    let fragment = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 0,
        root: values_distributed_node(0, 11, repeated_input.clone()),
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::IcebergWrite(
            crate::sql::planner::distributed::write::sink::IcebergWriteFragmentSink {
                descriptor_database: "db".to_string(),
                spec: sink_spec,
                input: crate::sql::planner::distributed::write::sink::IcebergWriteInputBinding::RootOutputByOrdinal,
            },
        ),
        output_exprs: None,
        output_columns: repeated_input,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };

    // The write output/target-schema is finalized in the sealed plan
    // (CGO-9C Task 3), so encode through a sealed distributed plan rather than
    // the bare fragment helper: the encoder maps the finalized contract 1:1.
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![fragment],
        root_fragment_id: 0,
        runtime_filter_graph: RuntimeFilterGraph::default(),
        edges: Vec::new(),
    };
    let encoded_plan = plan::encode_distributed_plan(&plan, empty_scan_bindings())
        .expect("encode distributed plan");
    let encoded = encoded_plan
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("write fragment");

    assert_eq!(encoded.output_exprs.len(), 2);
    let encoded_ids = encoded
        .output_exprs
        .iter()
        .map(|expr| {
            let Some(crate::proto::expr::expr::Kind::ColumnRef(column)) = expr.kind.as_ref() else {
                panic!("expected column ref");
            };
            column.column_id
        })
        .collect::<Vec<_>>();
    assert_eq!(encoded_ids, vec![7, 7]);

    let output_ids = encoded
        .output_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    assert_eq!(output_ids, vec![1, 2]);
    assert_eq!(
        encoded
            .output_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["c0", "c1"]
    );
}

#[test]
fn native_scan_encoder_preserves_iceberg_write_defaults() {
    let schema = crate::connector::iceberg::scan_model::IcebergSchemaDef {
        fields: vec![
            crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
                field_id: 1,
                name: "amount".to_string(),
                initial_default: Some(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int(5),
                )),
                write_default: Some(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int(7),
                )),
                initial_default_json: Some("5".to_string()),
                write_default_json: Some("7".to_string()),
                children: vec![],
            },
        ],
    };
    let iceberg_table = crate::connector::iceberg::scan_model::IcebergTableInfo {
        catalog: "ice".to_string(),
        namespace: "db".to_string(),
        table: "orders".to_string(),
        table_uuid: Some("uuid-orders".to_string()),
        current_snapshot_id: Some(10),
        schema_id: 1,
        location: "s3://warehouse/db/orders".to_string(),
        schema,
        serialized_metadata: None,
        serialized_metadata_rows: None,
    };
    let table = crate::sql::planner::table::TableDef {
        name: "orders".to_string(),
        columns: vec![novarocks_catalog::schema::ColumnDef {
            name: "amount".to_string(),
            data_type: DataType::Decimal128(10, 2),
            nullable: true,
            write_default: Some(ColumnDefault::Decimal {
                unscaled: 999,
                precision: 10,
                scale: 2,
            }),
            logical_type: None,
        }],
        iceberg_row_lineage_metadata_columns: vec![],
        source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: iceberg_table,
            files: vec![],
            cloud_properties: std::collections::BTreeMap::new(),
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        },
    };
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
                table,
                alias: None,
                columns: vec![output_column(10, "amount", DataType::Decimal128(10, 2))],
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            },
        ),
    };

    let encoded = plan::encode_node(&scan).expect("encode scan node");
    let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
        encoded.payload.as_ref()
    else {
        panic!("expected physical node");
    };
    let Some(crate::proto::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
        panic!("expected scan node");
    };
    let table = scan.table.as_ref().expect("table");

    assert_eq!(
        table.columns[0].write_default_json.as_deref(),
        Some("\"9.99\"")
    );
    let source = table.source.as_ref().expect("scan source");
    let Some(crate::proto::plan::scan_source::Kind::IcebergDataFiles(iceberg)) =
        source.kind.as_ref()
    else {
        panic!("expected Iceberg data-files source");
    };
    let field = &iceberg
        .table
        .as_ref()
        .expect("iceberg table")
        .schema
        .as_ref()
        .expect("iceberg schema")
        .fields[0];
    assert_eq!(field.initial_default_json.as_deref(), Some("5"));
    assert_eq!(field.write_default_json.as_deref(), Some("7"));
}

#[test]
fn native_scan_encoder_preserves_iceberg_list_write_defaults_from_arrow_metadata() {
    let list_type = DataType::List(Arc::new(
        Field::new("element", DataType::Int32, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "4".to_string(),
        )])),
    ));
    let table = crate::sql::planner::table::TableDef {
        name: "orders".to_string(),
        columns: vec![novarocks_catalog::schema::ColumnDef {
            name: "tags".to_string(),
            data_type: list_type.clone(),
            nullable: true,
            write_default: Some(ColumnDefault::Array(vec![])),
            logical_type: None,
        }],
        iceberg_row_lineage_metadata_columns: vec![],
        source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: crate::connector::iceberg::scan_model::IcebergTableInfo {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                table_uuid: Some("uuid-orders".to_string()),
                current_snapshot_id: Some(10),
                schema_id: 1,
                location: "s3://warehouse/db/orders".to_string(),
                schema: crate::connector::iceberg::scan_model::IcebergSchemaDef { fields: vec![] },
                serialized_metadata: None,
                serialized_metadata_rows: None,
            },
            files: vec![],
            cloud_properties: std::collections::BTreeMap::new(),
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        },
    };
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
                table,
                alias: None,
                columns: vec![output_column(10, "tags", list_type)],
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            },
        ),
    };

    let encoded = plan::encode_node(&scan).expect("encode scan node");
    let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
        encoded.payload.as_ref()
    else {
        panic!("expected physical node");
    };
    let Some(crate::proto::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
        panic!("expected scan node");
    };
    let table = scan.table.as_ref().expect("table");

    assert_eq!(table.columns[0].write_default_json.as_deref(), Some("[]"));
}

#[test]
fn iceberg_write_sink_preserves_position_delete_descriptor_order_and_fields() {
    use crate::connector::iceberg::position_delete_descriptor::{
        ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        PositionDeleteDescriptorInput, PositionDeleteOutputField,
        PositionDeletePartitionSourceField,
    };

    let mut spec = crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
    spec.position_delete_output_descriptor = Some(PositionDeleteDescriptorInput {
        file_path: PositionDeleteOutputField {
            output_expr_index: 0,
            name: "file_path".to_string(),
            data_type: DataType::Utf8,
            field_id: ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        },
        pos: PositionDeleteOutputField {
            output_expr_index: 1,
            name: "pos".to_string(),
            data_type: DataType::Int64,
            field_id: ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        },
        partition_source_fields: vec![PositionDeletePartitionSourceField {
            output_expr_index: 2,
            source_column_name: "event_date".to_string(),
            partition_field_name: "event_day".to_string(),
            transform_expr: "day(event_date)".to_string(),
            source_field_id: 17,
            data_type: DataType::Date32,
        }],
        target_partition_spec_id: 9,
    });
    let ctx = NativePlanEncodeContext {
        scan_bindings: None,
        node_outputs: None,
        fragment_edge_outputs: None,
        write_contracts: None,
        runtime_filter_bindings: None,
    };

    let encoded = encode_iceberg_write_sink_spec(&spec, &ctx)
        .expect("encode Iceberg position-delete sink descriptor");
    let descriptor = encoded
        .position_delete_output_descriptor
        .expect("position-delete descriptor");
    assert_eq!(descriptor.file_path.as_ref().unwrap().output_expr_index, 0);
    assert_eq!(descriptor.pos.as_ref().unwrap().output_expr_index, 1);
    assert_eq!(descriptor.partition_source_fields.len(), 1);
    assert_eq!(descriptor.partition_source_fields[0].output_expr_index, 2);
    assert_eq!(descriptor.partition_source_fields[0].source_field_id, 17);
    assert_eq!(descriptor.target_partition_spec_id, 9);
}
