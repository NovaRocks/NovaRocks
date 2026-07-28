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

use arrow::datatypes::DataType;
use prost::Message;

use super::super::plan;
use super::{column_expr, int_expr};
use crate::coordinator::prepare::scan::ScanExecutionBindings;
use crate::protocol::native::type_mapping::decode_type;
use crate::sql::column_id::ColumnId;

fn empty_scan_bindings() -> &'static ScanExecutionBindings {
    Box::leak(Box::new(ScanExecutionBindings::default()))
}

fn planner_output_column(
    id: u32,
    name: &str,
    data_type: DataType,
) -> crate::sql::analysis::OutputColumn {
    crate::sql::analysis::OutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: name.to_string(),
        data_type,
        nullable: false,
        is_internal: false,
    }
}

fn physical_stats() -> crate::sql::planner::physical::PhysicalPlanStats {
    crate::sql::planner::physical::PhysicalPlanStats {
        output_row_count: 0.0,
        row_count_confidence: crate::sql::planner::physical::PlannerConfidence::Fallback,
        column_statistics: std::collections::HashMap::new(),
        cost_estimate: None,
        broadcast_decision: None,
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
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Values(
            crate::sql::planner::payload::PlanValuesNode {
                rows: vec![vec![int_expr(7)]],
                columns: output,
            },
        ),
    }
}

fn iceberg_scan_table_for_columns(names: &[&str]) -> crate::sql::planner::table::TableDef {
    let columns = names
        .iter()
        .map(|name| novarocks_catalog::schema::ColumnDef {
            name: (*name).to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        })
        .collect::<Vec<_>>();
    let schema = crate::connector::iceberg::scan_model::IcebergSchemaDef {
        fields: names
            .iter()
            .enumerate()
            .map(
                |(idx, name)| crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
                    field_id: i32::try_from(idx + 1).expect("field id"),
                    name: (*name).to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                },
            )
            .collect(),
    };
    let iceberg_table = crate::connector::iceberg::scan_model::IcebergTableInfo {
        catalog: "ice".to_string(),
        namespace: "db".to_string(),
        table: "sc2".to_string(),
        table_uuid: Some("uuid-sc2".to_string()),
        current_snapshot_id: Some(10),
        schema_id: 1,
        location: "s3://warehouse/db/sc2".to_string(),
        schema,
        serialized_metadata: None,
        serialized_metadata_rows: None,
    };
    crate::sql::planner::table::TableDef {
        name: "sc2".to_string(),
        columns,
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: iceberg_table,
            files: Vec::new(),
            cloud_properties: std::collections::BTreeMap::new(),
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        },
    }
}

#[test]
fn distributed_plan_encoder_round_trips_fragments_edges_partitions_and_exchange() {
    let output = vec![planner_output_column(10, "v", DataType::Int64)];
    let source = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 0,
        root: values_distributed_node(0, 11, output.clone()),
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition {
            kind: crate::sql::planner::distributed::PartitionKind::Hash,
            exprs: vec![column_expr(10, "v", DataType::Int64)],
        },
        sink: crate::sql::planner::distributed::DataSink::Noop,
        output_exprs: None,
        output_columns: output.clone(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let receiver = crate::sql::planner::distributed::DistributedNode {
        node_id: 42,
        fragment_id: 1,
        tuple_ids: vec![42],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Exchange(
            crate::sql::planner::distributed::ExchangeReceiver {
                partition: crate::sql::planner::distributed::DataPartition::hash(vec![
                    column_expr(10, "v", DataType::Int64),
                ]),
                source_fragment_id: 0,
                output_columns: output.clone(),
                output_qualifier: Some("recv".to_string()),
                flavor: crate::sql::planner::distributed::ExchangeFlavor::Distribution,
            },
        ),
    };
    let target = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 1,
        root: receiver,
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Result,
        output_exprs: None,
        output_columns: output,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![source, target],
        root_fragment_id: 1,
        runtime_filter_graph: Default::default(),
        edges: vec![crate::sql::planner::distributed::FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 42,
            output_partition: crate::sql::planner::distributed::DataPartition {
                kind: crate::sql::planner::distributed::PartitionKind::Hash,
                exprs: vec![column_expr(10, "v", DataType::Int64)],
            },
            stream_kind: crate::sql::planner::distributed::FragmentStreamKind::Partitioned,
            edge_kind: crate::sql::planner::distributed::FragmentEdgeKind::Stream,
            output_slot_ids: vec![10],
        }],
    };

    let encoded = plan::encode_distributed_plan(&plan, empty_scan_bindings())
        .expect("encode distributed plan");
    let decoded = crate::proto::plan::DistributedPlan::decode(encoded.encode_to_vec().as_slice())
        .expect("decode proto message");

    assert_eq!(decoded.root_fragment_id, 1);
    assert_eq!(decoded.fragments.len(), 2);
    assert_eq!(decoded.edges.len(), 1);
    assert_eq!(decoded.edges[0].target_exchange_node_id, 42);
    assert_eq!(
        decoded.edges[0].output_partition,
        crate::proto::plan::PartitionType::Hash as i32
    );
    assert_eq!(
        decoded.edges[0]
            .edge_kind
            .as_ref()
            .and_then(|kind| kind.kind.as_ref()),
        Some(&crate::proto::plan::fragment_edge_kind::Kind::Stream(true))
    );

    let root_fragment = decoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("root fragment");
    // Sealed plans never carry fragment `output_exprs` (rejected by
    // structural validation), so the round-trip only covers the shapes a
    // production plan can actually hold: fragments, edges, partitions, and
    // the exchange receiver.
    let root = root_fragment.root.as_ref().expect("root node");
    let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
        root.payload.as_ref()
    else {
        panic!("expected exchange receiver payload");
    };
    assert_eq!(exchange.source_fragment_id, 0);
    assert_eq!(exchange.output_qualifier.as_deref(), Some("recv"));
    assert_eq!(
        exchange.partition_type,
        crate::proto::plan::PartitionType::Hash as i32
    );
    assert_eq!(exchange.output_columns.len(), 1);
    assert_eq!(exchange.output_columns[0].column_id, 10);
    assert_eq!(exchange.output_columns[0].name, "v");
}

#[test]
fn stream_edge_projects_pruned_scan_columns_by_column_id() {
    let all_scan_columns = vec![
        planner_output_column(1, "v1", DataType::Int64),
        planner_output_column(2, "s2", DataType::Utf8),
        planner_output_column(3, "array1", DataType::Int64),
    ];
    let stream_columns = vec![all_scan_columns[1].clone(), all_scan_columns[2].clone()];
    let source_root = crate::sql::planner::distributed::DistributedNode {
        node_id: 11,
        fragment_id: 0,
        tuple_ids: vec![11],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Scan(
            crate::sql::planner::payload::PlanScanNode {
                database: "db".to_string(),
                table: iceberg_scan_table_for_columns(&["v1", "s2", "array1"]),
                alias: None,
                columns: all_scan_columns,
                predicates: Vec::new(),
                required_columns: Some(vec!["s2".to_string(), "array1".to_string()]),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            },
        ),
    };
    let source = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 0,
        root: source_root,
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Noop,
        output_exprs: None,
        output_columns: stream_columns.clone(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let receiver = crate::sql::planner::distributed::DistributedNode {
        node_id: 42,
        fragment_id: 1,
        tuple_ids: vec![42],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Exchange(
            crate::sql::planner::distributed::ExchangeReceiver {
                partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
                source_fragment_id: 0,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: crate::sql::planner::distributed::ExchangeFlavor::Distribution,
            },
        ),
    };
    let target = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 1,
        root: receiver,
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Result,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![source, target],
        root_fragment_id: 1,
        runtime_filter_graph: Default::default(),
        edges: vec![crate::sql::planner::distributed::FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 42,
            output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
            stream_kind: crate::sql::planner::distributed::FragmentStreamKind::Gather,
            edge_kind: crate::sql::planner::distributed::FragmentEdgeKind::Stream,
            output_slot_ids: vec![2, 3],
        }],
    };

    let encoded = plan::encode_distributed_plan(&plan, empty_scan_bindings())
        .expect("encode distributed plan");
    let target_fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("target fragment");
    let root = target_fragment.root.as_ref().expect("target root");
    let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
        root.payload.as_ref()
    else {
        panic!("expected exchange receiver payload");
    };

    let patched = exchange
        .output_columns
        .iter()
        .map(|column| (column.column_id, column.name.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(patched, vec![(2, "s2"), (3, "array1")]);
}

#[test]
fn stream_edge_patches_exchange_columns_from_aggregate_layout_when_fragment_output_is_empty() {
    let group_column = planner_output_column(2, "c1", DataType::Utf8);
    let source_root = crate::sql::planner::distributed::DistributedNode {
        node_id: 11,
        fragment_id: 0,
        tuple_ids: vec![11],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_distributed_node(0, 10, vec![group_column.clone()])],
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::HashAggregate(Box::new(
            crate::sql::planner::physical::PhysicalHashAggregateNode {
                mode: crate::sql::planner::physical::AggMode::Local,
                group_by: vec![column_expr(2, "c1", DataType::Utf8)],
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_layout: crate::sql::planner::physical::AggregateOutputLayout::new(
                    vec![group_column.clone()],
                    Vec::new(),
                ),
                output_columns: Vec::new(),
                topn_runtime_filter_builds: Vec::new(),
            },
        )),
    };
    let source = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 0,
        root: source_root,
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Noop,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let receiver = crate::sql::planner::distributed::DistributedNode {
        node_id: 42,
        fragment_id: 1,
        tuple_ids: vec![42],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Exchange(
            crate::sql::planner::distributed::ExchangeReceiver {
                partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
                source_fragment_id: 0,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: crate::sql::planner::distributed::ExchangeFlavor::Distribution,
            },
        ),
    };
    let target = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 1,
        root: receiver,
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Result,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![source, target],
        root_fragment_id: 1,
        runtime_filter_graph: Default::default(),
        edges: vec![crate::sql::planner::distributed::FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 42,
            output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
            stream_kind: crate::sql::planner::distributed::FragmentStreamKind::Gather,
            edge_kind: crate::sql::planner::distributed::FragmentEdgeKind::Stream,
            output_slot_ids: vec![2],
        }],
    };

    let encoded = plan::encode_distributed_plan(&plan, empty_scan_bindings())
        .expect("encode distributed plan");
    let target_fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("target fragment");
    let root = target_fragment.root.as_ref().expect("target root");
    let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
        root.payload.as_ref()
    else {
        panic!("expected exchange receiver");
    };
    assert_eq!(exchange.output_columns.len(), 1);
    assert_eq!(exchange.output_columns[0].column_id, 2);
    assert_eq!(exchange.output_columns[0].name, "c1");
}

#[test]
fn stream_edge_patches_local_avg_exchange_schema_to_intermediate_type() {
    let group_column = planner_output_column(2, "c0", DataType::Int64);
    let value_column = planner_output_column(3, "c1", DataType::Int64);
    let avg_column = planner_output_column(15, "avg(c1)", DataType::Float64);
    let source_root = crate::sql::planner::distributed::DistributedNode {
        node_id: 11,
        fragment_id: 0,
        tuple_ids: vec![11],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_distributed_node(
            0,
            10,
            vec![group_column.clone(), value_column.clone()],
        )],
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::HashAggregate(Box::new(
            crate::sql::planner::physical::PhysicalHashAggregateNode {
                mode: crate::sql::planner::physical::AggMode::Local,
                group_by: vec![column_expr(2, "c0", DataType::Int64)],
                aggregates: vec![crate::sql::planner::payload::AggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_expr(3, "c1", DataType::Int64)],
                    distinct: false,
                    result_type: DataType::Float64,
                    order_by: Vec::new(),
                    output_column_id: crate::sql::column_id::ColumnId::new_for_test(15),
                }],
                is_merge: vec![false],
                output_layout: crate::sql::planner::physical::AggregateOutputLayout::new(
                    vec![group_column.clone()],
                    vec![avg_column.clone()],
                ),
                output_columns: Vec::new(),
                topn_runtime_filter_builds: Vec::new(),
            },
        )),
    };
    let source = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 0,
        root: source_root,
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Noop,
        output_exprs: None,
        output_columns: vec![group_column.clone(), avg_column],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let receiver = crate::sql::planner::distributed::DistributedNode {
        node_id: 42,
        fragment_id: 1,
        tuple_ids: vec![42],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Exchange(
            crate::sql::planner::distributed::ExchangeReceiver {
                partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
                source_fragment_id: 0,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: crate::sql::planner::distributed::ExchangeFlavor::Distribution,
            },
        ),
    };
    let target = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 1,
        root: receiver,
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Result,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![source, target],
        root_fragment_id: 1,
        runtime_filter_graph: Default::default(),
        edges: vec![crate::sql::planner::distributed::FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 42,
            output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
            stream_kind: crate::sql::planner::distributed::FragmentStreamKind::Gather,
            edge_kind: crate::sql::planner::distributed::FragmentEdgeKind::Stream,
            output_slot_ids: vec![2, 15],
        }],
    };

    let encoded = plan::encode_distributed_plan(&plan, empty_scan_bindings())
        .expect("encode distributed plan");
    let target_fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("target fragment");
    let root = target_fragment.root.as_ref().expect("target root");
    let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
        root.payload.as_ref()
    else {
        panic!("expected exchange receiver");
    };
    assert_eq!(exchange.output_columns.len(), 2);
    assert_eq!(exchange.output_columns[1].column_id, 15);
    let avg_type = decode_type(
        exchange.output_columns[1]
            .r#type
            .as_ref()
            .expect("avg column type"),
    )
    .expect("decode avg column type");
    assert_eq!(avg_type, DataType::Utf8);
}

#[test]
fn stream_edge_allows_zero_column_source_when_no_slots_are_requested() {
    let source = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 0,
        root: values_distributed_node(0, 10, Vec::new()),
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Noop,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let receiver = crate::sql::planner::distributed::DistributedNode {
        node_id: 42,
        fragment_id: 1,
        tuple_ids: vec![42],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Exchange(
            crate::sql::planner::distributed::ExchangeReceiver {
                partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
                source_fragment_id: 0,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: crate::sql::planner::distributed::ExchangeFlavor::Distribution,
            },
        ),
    };
    let target = crate::sql::planner::distributed::PlanFragment {
        fragment_id: 1,
        root: receiver,
        data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Result,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![source, target],
        root_fragment_id: 1,
        runtime_filter_graph: Default::default(),
        edges: vec![crate::sql::planner::distributed::FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 42,
            output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
            stream_kind: crate::sql::planner::distributed::FragmentStreamKind::Gather,
            edge_kind: crate::sql::planner::distributed::FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }],
    };

    let encoded = plan::encode_distributed_plan(&plan, empty_scan_bindings())
        .expect("encode distributed plan");
    let target_fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("target fragment");
    let root = target_fragment.root.as_ref().expect("target root");
    let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
        root.payload.as_ref()
    else {
        panic!("expected exchange receiver");
    };
    assert!(exchange.output_columns.is_empty());
}
