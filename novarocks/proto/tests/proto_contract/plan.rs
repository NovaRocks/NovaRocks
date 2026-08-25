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

use prost::Message;

use novarocks_proto::{common, expr, plan};

#[derive(Clone, Debug, PartialEq, Eq)]
struct IPlan {
    root_fragment_id: u32,
    fragments: Vec<IFragment>,
    edges: Vec<IEdge>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IFragment {
    fragment_id: u32,
    root: INode,
    sink_result: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct INode {
    node_id: i32,
    fragment_id: u32,
    payload: IPayload,
    children: Vec<INode>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IPayload {
    Scan {
        database: String,
        table: String,
        file_count: usize,
    },
    HashJoin {
        runtime_filter_id: i32,
    },
    Exchange {
        source_fragment_id: u32,
    },
    Redistribute {
        cols: Vec<u32>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IEdge {
    source_fragment_id: u32,
    target_fragment_id: u32,
    target_exchange_node_id: i32,
}

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

fn scalar_type(prim: common::PrimitiveType) -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: prim as i32,
            len: None,
            precision: None,
            scale: None,
            time_unit: None,
        })),
    }
}

fn output_column(column_id: u32, name: &str, prim: common::PrimitiveType) -> common::OutputColumn {
    common::OutputColumn {
        column_id,
        name: name.to_string(),
        r#type: Some(scalar_type(prim)),
        nullable: false,
        is_internal: false,
    }
}

fn column_def(name: &str, prim: common::PrimitiveType) -> plan::ColumnDef {
    plan::ColumnDef {
        name: name.to_string(),
        data_type: Some(scalar_type(prim)),
        nullable: false,
        write_default_json: None,
        logical_type: None,
    }
}

fn column_expr(column_id: u32) -> expr::Expr {
    expr::Expr {
        r#type: Some(scalar_type(common::PrimitiveType::Bigint)),
        nullable: false,
        kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
            column_id,
            qualifier: Some("t".to_string()),
            column: Some(format!("c{column_id}")),
        })),
    }
}

fn literal_bool(value: bool) -> expr::Expr {
    expr::Expr {
        r#type: Some(scalar_type(common::PrimitiveType::Boolean)),
        nullable: false,
        kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
            value: Some(common::LiteralValue {
                value: Some(common::literal_value::Value::BoolValue(value)),
            }),
        })),
    }
}

fn iceberg_table(table: &str, file_count: usize) -> plan::TableDef {
    plan::TableDef {
        name: table.to_string(),
        columns: vec![column_def("id", common::PrimitiveType::Bigint)],
        iceberg_row_lineage_metadata_columns: vec![],
        source: Some(plan::ScanSource {
            kind: Some(plan::scan_source::Kind::ConnectorRead(
                plan::ConnectorReadSource {
                    instance_id: "rest".to_string(),
                    instance_incarnation: vec![1; 16],
                    scan_payload: table.as_bytes().to_vec(),
                    splits: (0..file_count)
                        .map(|idx| plan::ConnectorReadSplit {
                            split_id: format!("{table}-{idx}"),
                            split_payload: Vec::new(),
                            estimated_bytes: Some(1024),
                        })
                        .collect(),
                    max_batch_rows: 1024,
                    max_batch_bytes: 1_048_576,
                    max_handle_payload_bytes: 1_048_576,
                    max_total_payload_bytes: 8_388_608,
                    expected_schema_ipc: Vec::new(),
                },
            )),
        }),
    }
}

fn data_partition() -> plan::DataPartition {
    plan::DataPartition {
        kind: plan::PartitionKind::Hash as i32,
        exprs: vec![column_expr(1)],
    }
}

fn sink(result: bool) -> plan::DataSink {
    plan::DataSink {
        kind: Some(if result {
            plan::data_sink::Kind::Result(true)
        } else {
            plan::data_sink::Kind::Noop(true)
        }),
    }
}

fn node_to_proto(node: &INode) -> plan::DistributedNode {
    let payload = match &node.payload {
        IPayload::Scan {
            database,
            table,
            file_count,
        } => plan::distributed_node::Payload::Physical(plan::PlanNode {
            output_columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
            kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                database: database.clone(),
                table: Some(iceberg_table(table, *file_count)),
                alias: Some("t".to_string()),
                columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
                predicates: vec![literal_bool(true)],
                required_columns: vec!["id".to_string()],
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            })),
        }),
        IPayload::HashJoin { .. } => plan::distributed_node::Payload::Physical(plan::PlanNode {
            output_columns: vec![output_column(3, "joined_id", common::PrimitiveType::Bigint)],
            kind: Some(plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_expr(1)),
                    right: Some(column_expr(2)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Shuffle as i32,
                execution_mode: Some(plan::JoinExecutionMode::Partitioned as i32),
            })),
        }),
        IPayload::Exchange { source_fragment_id } => {
            plan::distributed_node::Payload::Exchange(plan::ExchangeReceiver {
                partition_type: plan::PartitionType::Hash as i32,
                partition_exprs: vec![column_expr(1)],
                source_fragment_id: *source_fragment_id,
                output_columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
                output_qualifier: Some("remote".to_string()),
                flavor: Some(plan::ExchangeFlavor {
                    kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                }),
            })
        }
        IPayload::Redistribute { cols } => {
            plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
                kind: Some(plan::plan_node::Kind::Redistribute(
                    plan::RedistributeNode {
                        mode: Some(plan::RedistributeMode {
                            mode: Some(plan::redistribute_mode::Mode::Hash(
                                plan::RedistributeHash {
                                    cols: cols.clone(),
                                    source: plan::HashSource::ShuffleJoin as i32,
                                },
                            )),
                        }),
                        partition_exprs: vec![column_expr(1)],
                        output_columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
                    },
                )),
            })
        }
    };

    plan::DistributedNode {
        node_id: node.node_id,
        fragment_id: node.fragment_id,
        tuple_ids: vec![node.node_id],
        nullable_tuple_ids: vec![],
        limit: -1,
        runtime_filter_binding_ids: match &node.payload {
            IPayload::HashJoin { runtime_filter_id } => vec![
                u32::try_from(*runtime_filter_id)
                    .expect("runtime-filter fixture id must fit a binding id"),
            ],
            _ => Vec::new(),
        },
        children: node.children.iter().map(node_to_proto).collect(),
        payload: Some(payload),
    }
}

fn node_from_proto(proto: &plan::DistributedNode) -> Result<INode, String> {
    let payload = match proto
        .payload
        .as_ref()
        .ok_or("DistributedNode.payload missing")?
    {
        plan::distributed_node::Payload::Physical(plan_node) => {
            match plan_node.kind.as_ref().ok_or("PlanNode.kind missing")? {
                plan::plan_node::Kind::Scan(scan) => {
                    let table = scan.table.as_ref().ok_or("ScanNode.table missing")?;
                    let source = table.source.as_ref().ok_or("TableDef.source missing")?;
                    let plan::scan_source::Kind::ConnectorRead(source) =
                        source.kind.as_ref().ok_or("ScanSource.kind missing")?;
                    let file_count = source.splits.len();
                    IPayload::Scan {
                        database: scan.database.clone(),
                        table: table.name.clone(),
                        file_count,
                    }
                }
                plan::plan_node::Kind::HashJoin(_) => IPayload::HashJoin {
                    runtime_filter_id: i32::try_from(
                        *proto
                            .runtime_filter_binding_ids
                            .first()
                            .ok_or("DistributedNode.runtime_filter_binding_ids missing")?,
                    )
                    .map_err(|_| "runtime-filter binding id does not fit i32".to_string())?,
                },
                plan::plan_node::Kind::Redistribute(redistribute) => {
                    let mode = redistribute
                        .mode
                        .as_ref()
                        .and_then(|mode| mode.mode.as_ref())
                        .ok_or("RedistributeNode.mode missing")?;
                    let cols = match mode {
                        plan::redistribute_mode::Mode::Hash(hash) => hash.cols.clone(),
                        other => return Err(format!("unexpected redistribute mode: {other:?}")),
                    };
                    IPayload::Redistribute { cols }
                }
                other => return Err(format!("unexpected physical node: {other:?}")),
            }
        }
        plan::distributed_node::Payload::Exchange(exchange) => IPayload::Exchange {
            source_fragment_id: exchange.source_fragment_id,
        },
    };

    Ok(INode {
        node_id: proto.node_id,
        fragment_id: proto.fragment_id,
        payload,
        children: proto
            .children
            .iter()
            .map(node_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn fragment_to_proto(fragment: &IFragment) -> plan::PlanFragment {
    plan::PlanFragment {
        fragment_id: fragment.fragment_id,
        root: Some(node_to_proto(&fragment.root)),
        data_partition: Some(data_partition()),
        output_partition: Some(data_partition()),
        sink: Some(sink(fragment.sink_result)),
        output_exprs: vec![column_expr(1)],
        output_columns: vec![output_column(1, "id", common::PrimitiveType::Bigint)],
        cte_id: None,
        cte_exchange_nodes: vec![],
        runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
            fragment_id: fragment.fragment_id,
            bindings: vec![],
        }),
    }
}

fn fragment_from_proto(proto: &plan::PlanFragment) -> Result<IFragment, String> {
    let sink_result = match proto
        .sink
        .as_ref()
        .and_then(|sink| sink.kind.as_ref())
        .ok_or("PlanFragment.sink missing")?
    {
        plan::data_sink::Kind::Result(value) => *value,
        plan::data_sink::Kind::Noop(_) => false,
        other => return Err(format!("unexpected sink: {other:?}")),
    };

    Ok(IFragment {
        fragment_id: proto.fragment_id,
        root: node_from_proto(proto.root.as_ref().ok_or("PlanFragment.root missing")?)?,
        sink_result,
    })
}

fn plan_to_proto(plan: &IPlan) -> plan::DistributedPlan {
    plan::DistributedPlan {
        fragments: plan.fragments.iter().map(fragment_to_proto).collect(),
        root_fragment_id: plan.root_fragment_id,
        edges: plan
            .edges
            .iter()
            .map(|edge| plan::FragmentEdge {
                source_fragment_id: edge.source_fragment_id,
                target_fragment_id: edge.target_fragment_id,
                target_exchange_node_id: edge.target_exchange_node_id,
                output_partition: plan::PartitionType::Hash as i32,
                stream_kind: plan::FragmentStreamKind::Partitioned as i32,
                edge_kind: Some(plan::FragmentEdgeKind {
                    kind: Some(plan::fragment_edge_kind::Kind::Stream(true)),
                }),
                output_slot_ids: vec![1],
            })
            .collect(),
    }
}

fn plan_from_proto(proto: &plan::DistributedPlan) -> Result<IPlan, String> {
    Ok(IPlan {
        root_fragment_id: proto.root_fragment_id,
        fragments: proto
            .fragments
            .iter()
            .map(fragment_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
        edges: proto
            .edges
            .iter()
            .map(|edge| IEdge {
                source_fragment_id: edge.source_fragment_id,
                target_fragment_id: edge.target_fragment_id,
                target_exchange_node_id: edge.target_exchange_node_id,
            })
            .collect(),
    })
}

fn sample_internal_plan() -> IPlan {
    IPlan {
        root_fragment_id: 1,
        fragments: vec![
            IFragment {
                fragment_id: 1,
                sink_result: true,
                root: INode {
                    node_id: 10,
                    fragment_id: 1,
                    payload: IPayload::HashJoin {
                        runtime_filter_id: 77,
                    },
                    children: vec![
                        INode {
                            node_id: 11,
                            fragment_id: 1,
                            payload: IPayload::Scan {
                                database: "tpch".to_string(),
                                table: "orders".to_string(),
                                file_count: 2,
                            },
                            children: vec![],
                        },
                        INode {
                            node_id: 12,
                            fragment_id: 1,
                            payload: IPayload::Exchange {
                                source_fragment_id: 2,
                            },
                            children: vec![],
                        },
                    ],
                },
            },
            IFragment {
                fragment_id: 2,
                sink_result: false,
                root: INode {
                    node_id: 20,
                    fragment_id: 2,
                    payload: IPayload::Redistribute { cols: vec![1] },
                    children: vec![INode {
                        node_id: 21,
                        fragment_id: 2,
                        payload: IPayload::Scan {
                            database: "tpch".to_string(),
                            table: "lineitem".to_string(),
                            file_count: 1,
                        },
                        children: vec![],
                    }],
                },
            },
        ],
        edges: vec![IEdge {
            source_fragment_id: 2,
            target_fragment_id: 1,
            target_exchange_node_id: 12,
        }],
    }
}

fn classify_plan_node_kind(kind: plan::plan_node::Kind) -> &'static str {
    match kind {
        plan::plan_node::Kind::Scan(_) => "scan",
        plan::plan_node::Kind::Filter(_) => "filter",
        plan::plan_node::Kind::Project(_) => "project",
        plan::plan_node::Kind::Sort(_) => "sort",
        plan::plan_node::Kind::Limit(_) => "limit",
        plan::plan_node::Kind::Values(_) => "values",
        plan::plan_node::Kind::Decode(_) => "decode",
        plan::plan_node::Kind::Repeat(_) => "repeat",
        plan::plan_node::Kind::Window(_) => "window",
        plan::plan_node::Kind::GenerateSeries(_) => "generate_series",
        plan::plan_node::Kind::TableFunction(_) => "table_function",
        plan::plan_node::Kind::AssertOneRow(_) => "assert_one_row",
        plan::plan_node::Kind::Topn(_) => "topn",
        plan::plan_node::Kind::HashAggregate(_) => "hash_aggregate",
        plan::plan_node::Kind::HashJoin(_) => "hash_join",
        plan::plan_node::Kind::NestLoopJoin(_) => "nest_loop_join",
        plan::plan_node::Kind::SetOp(_) => "set_op",
        plan::plan_node::Kind::ChangeEventExpand(_) => "change_event_expand",
        plan::plan_node::Kind::CteAnchor(_) => "cte_anchor",
        plan::plan_node::Kind::CteProduce(_) => "cte_produce",
        plan::plan_node::Kind::CteConsume(_) => "cte_consume",
        plan::plan_node::Kind::Redistribute(_) => "redistribute",
    }
}

#[test]
fn distributed_plan_survives_proto_roundtrip() {
    let original = sample_internal_plan();
    let proto = plan_to_proto(&original);

    let decoded: plan::DistributedPlan = roundtrip_message(&proto);
    assert_eq!(proto, decoded);

    let back = plan_from_proto(&decoded).expect("convert DistributedPlan back");
    assert_eq!(original, back);
}

#[test]
fn missing_plan_payload_reports_boundary_error() {
    let proto = plan::DistributedNode {
        node_id: 1,
        fragment_id: 1,
        tuple_ids: vec![],
        nullable_tuple_ids: vec![],
        limit: -1,
        runtime_filter_binding_ids: vec![],
        children: vec![],
        payload: None,
    };

    assert_eq!(
        node_from_proto(&proto).expect_err("missing payload"),
        "DistributedNode.payload missing"
    );
}

#[test]
fn plan_node_kind_match_is_exhaustive_over_current_oneof() {
    use plan::plan_node::Kind;

    let kinds = vec![
        Kind::Scan(plan::ScanNode::default()),
        Kind::Filter(plan::FilterNode::default()),
        Kind::Project(plan::ProjectNode::default()),
        Kind::Sort(plan::SortNode::default()),
        Kind::Limit(plan::LimitNode::default()),
        Kind::Values(plan::ValuesNode::default()),
        Kind::Decode(plan::DecodeNode::default()),
        Kind::Repeat(plan::RepeatNode::default()),
        Kind::Window(plan::WindowNode::default()),
        Kind::GenerateSeries(plan::GenerateSeriesNode::default()),
        Kind::TableFunction(plan::TableFunctionNode::default()),
        Kind::AssertOneRow(plan::AssertOneRowNode::default()),
        Kind::Topn(plan::TopNNode::default()),
        Kind::HashAggregate(plan::HashAggregateNode::default()),
        Kind::HashJoin(plan::HashJoinNode::default()),
        Kind::NestLoopJoin(plan::NestLoopJoinNode::default()),
        Kind::SetOp(plan::SetOpNode::default()),
        Kind::ChangeEventExpand(plan::ChangeEventExpandNode::default()),
        Kind::CteAnchor(plan::CteAnchorNode::default()),
        Kind::CteProduce(plan::CteProduceNode::default()),
        Kind::CteConsume(plan::CteConsumeNode::default()),
        Kind::Redistribute(plan::RedistributeNode::default()),
    ];

    let names = kinds
        .into_iter()
        .map(classify_plan_node_kind)
        .collect::<Vec<_>>();
    assert_eq!(names.len(), 22);
    assert!(names.contains(&"scan"));
    assert!(names.contains(&"redistribute"));
}
