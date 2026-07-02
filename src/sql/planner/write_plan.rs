use crate::sql::codegen::{FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind};
use crate::sql::planner::plan::ExchangeFlavor;
use crate::sql::planner::{
    ChangeStreamWriteDagSpec, DataPartition, DataSink, DistributedNode, DistributedPayload,
    ExchangeReceiver, IcebergChangeStreamBranchRoute, IcebergChangeStreamRouterSink,
    IcebergChangeStreamWriteTopology, IcebergChangeStreamWriterBranch, IcebergWriteFragmentSink,
    IcebergWriteInputBinding, PlanFragment, PlannedIcebergChangeStreamDistributedPlan,
};
use crate::thrift::partitions;

pub(crate) fn with_iceberg_write_sink(
    mut plan: crate::sql::planner::DistributedPlan,
    sink: crate::sql::planner::IcebergWriteFragmentSink,
) -> Result<crate::sql::planner::DistributedPlan, String> {
    let root_fragment_id = plan.root_fragment_id;
    let root = plan
        .fragments
        .iter_mut()
        .find(|fragment| fragment.fragment_id == root_fragment_id)
        .ok_or_else(|| {
            format!("Iceberg write sink cannot find root fragment id={root_fragment_id}")
        })?;
    if !matches!(root.sink, crate::sql::planner::DataSink::Result) {
        return Err(format!(
            "Iceberg write sink expected root fragment id={} to use result sink",
            root.fragment_id
        ));
    }
    validate_iceberg_sink_arity(root, &sink)?;
    root.sink = crate::sql::planner::DataSink::IcebergWrite(sink);
    Ok(plan)
}

fn validate_iceberg_sink_arity(
    fragment: &crate::sql::planner::PlanFragment,
    sink: &crate::sql::planner::IcebergWriteFragmentSink,
) -> Result<(), String> {
    let input_count = match &sink.input {
        crate::sql::planner::IcebergWriteInputBinding::RootOutputByOrdinal => {
            fragment.output_columns.len()
        }
        crate::sql::planner::IcebergWriteInputBinding::OutputOrdinals(ordinals) => {
            validate_iceberg_sink_output_ordinals(&fragment.output_columns, ordinals)?;
            ordinals.len()
        }
    };
    if input_count != sink.spec.target_columns.len() {
        return Err(format!(
            "Iceberg write sink input column count {} does not match target column count {}",
            input_count,
            sink.spec.target_columns.len()
        ));
    }
    Ok(())
}

fn validate_iceberg_sink_output_ordinals(
    output_columns: &[crate::sql::analysis::OutputColumn],
    ordinals: &[usize],
) -> Result<(), String> {
    for ordinal in ordinals {
        if output_columns.get(*ordinal).is_none() {
            return Err(format!(
                "Iceberg write sink output ordinal {ordinal} is out of range"
            ));
        }
    }
    Ok(())
}

pub(crate) fn with_iceberg_change_stream_write(
    mut plan: crate::sql::planner::DistributedPlan,
    descriptor_database: &str,
    dag: ChangeStreamWriteDagSpec,
) -> Result<PlannedIcebergChangeStreamDistributedPlan, String> {
    dag.validate()?;
    if dag.branches.is_empty() {
        return Err("Iceberg change-stream write DAG requires at least one branch".to_string());
    }
    let change_op_output_ordinal = dag.change_op_output_ordinal.ok_or_else(|| {
        "Iceberg change-stream write DAG requires change_op_output_ordinal".to_string()
    })?;

    let root_fragment_id = plan.root_fragment_id;
    let root_index = plan
        .fragments
        .iter()
        .position(|fragment| fragment.fragment_id == root_fragment_id)
        .ok_or_else(|| {
            format!("Iceberg change-stream write cannot find root fragment id={root_fragment_id}")
        })?;
    if !matches!(plan.fragments[root_index].sink, DataSink::Result) {
        return Err(format!(
            "Iceberg change-stream write expected root fragment id={} to use result sink",
            root_fragment_id
        ));
    }

    let source_fragment = plan.fragments[root_index].clone();
    validate_output_ordinal(
        &source_fragment.output_columns,
        change_op_output_ordinal,
        "change_op",
    )?;
    if let Some(data_route_ordinal) = dag.data_route_output_ordinal {
        validate_output_ordinal(
            &source_fragment.output_columns,
            data_route_ordinal,
            "data_route",
        )?;
    }

    let mut next_fragment_id = next_fragment_id(&plan);
    let mut next_node_id = next_node_id(&plan);
    let mut next_tuple_id = next_tuple_id(&plan);
    let mut routes = Vec::with_capacity(dag.branches.len());
    let mut writer_branches = Vec::with_capacity(dag.branches.len());
    let mut writer_fragments = Vec::with_capacity(dag.branches.len());
    let mut writer_edges = Vec::with_capacity(dag.branches.len());

    for (branch_index, branch) in dag.branches.into_iter().enumerate() {
        validate_output_ordinals(
            &source_fragment.output_columns,
            &branch.stream_output_ordinals,
            &format!("branch {:?} output", branch.branch_kind),
        )?;
        validate_output_ordinals(
            &source_fragment.output_columns,
            &branch.output_partition_ordinals,
            &format!("branch {:?} partition", branch.branch_kind),
        )?;

        let mut sink_spec = branch.sink_spec;
        let table_id_offset = i64::try_from(branch_index).map_err(|_| {
            "Iceberg change-stream branch index overflow while assigning sink table ids".to_string()
        })?;
        sink_spec.target_table_id = crate::sql::planner::synthetic_iceberg_write_table_id()
            .checked_sub(table_id_offset)
            .ok_or_else(|| "Iceberg change-stream synthetic sink table id underflow".to_string())?;

        let writer_columns = output_columns_by_ordinals(
            &source_fragment.output_columns,
            &branch.stream_output_ordinals,
        )?;
        if writer_columns.len() != sink_spec.target_columns.len() {
            return Err(format!(
                "Iceberg change-stream branch {:?} output column count {} does not match target column count {}",
                branch.branch_kind,
                writer_columns.len(),
                sink_spec.target_columns.len()
            ));
        }

        let writer_fragment_id = next_fragment_id;
        next_fragment_id += 1;
        let exchange_node_id = next_node_id;
        next_node_id += 1;
        let exchange_tuple_id = next_tuple_id;
        next_tuple_id += 1;
        let partition_type = partition_type_for_ordinals(&branch.output_partition_ordinals);
        let stream_kind = stream_kind_for_partition_type(partition_type);

        writer_fragments.push(PlanFragment {
            fragment_id: writer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: writer_fragment_id,
                tuple_ids: vec![exchange_tuple_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: source_fragment.root.stats.clone(),
                payload: DistributedPayload::Exchange(ExchangeReceiver {
                    partition_type,
                    partition_exprs: Vec::new(),
                    source_fragment_id: root_fragment_id,
                    output_columns: writer_columns.clone(),
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::IcebergWrite(IcebergWriteFragmentSink {
                descriptor_database: descriptor_database.to_string(),
                spec: sink_spec.clone(),
                input: IcebergWriteInputBinding::RootOutputByOrdinal,
            }),
            output_exprs: None,
            output_columns: writer_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        });

        writer_edges.push(FragmentEdge {
            source_fragment_id: root_fragment_id,
            target_fragment_id: writer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: tdata_partition_placeholder(partition_type),
            stream_kind,
            edge_kind: FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id: 0,
                branch_id: branch.branch_id,
                branch_kind: branch.branch_kind,
            },
            output_slot_ids: Vec::new(),
        });

        routes.push(IcebergChangeStreamBranchRoute {
            branch_id: branch.branch_id,
            branch_kind: branch.branch_kind,
            target_fragment_id: writer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_ordinals: branch.stream_output_ordinals,
            output_partition_ordinals: branch.output_partition_ordinals,
        });
        writer_branches.push(IcebergChangeStreamWriterBranch {
            branch_id: branch.branch_id,
            branch_kind: branch.branch_kind,
            writer_fragment_id,
            sink_spec,
        });
    }

    plan.fragments[root_index].sink =
        DataSink::IcebergChangeStreamRouter(IcebergChangeStreamRouterSink {
            group_id: 0,
            change_op_output_ordinal,
            data_route_output_ordinal: dag.data_route_output_ordinal,
            branches: routes,
        });
    plan.fragments.extend(writer_fragments);
    plan.edges.extend(writer_edges);

    Ok(PlannedIcebergChangeStreamDistributedPlan {
        distributed_plan: plan,
        topology: IcebergChangeStreamWriteTopology { writer_branches },
    })
}

fn next_fragment_id(plan: &crate::sql::planner::DistributedPlan) -> FragmentId {
    plan.fragments
        .iter()
        .map(|fragment| fragment.fragment_id)
        .max()
        .unwrap_or_default()
        + 1
}

fn next_node_id(plan: &crate::sql::planner::DistributedPlan) -> i32 {
    plan.fragments
        .iter()
        .flat_map(|fragment| node_ids(&fragment.root))
        .max()
        .unwrap_or_default()
        + 1
}

fn next_tuple_id(plan: &crate::sql::planner::DistributedPlan) -> i32 {
    plan.fragments
        .iter()
        .flat_map(|fragment| node_tuple_ids(&fragment.root))
        .max()
        .unwrap_or_default()
        + 1
}

fn node_ids(node: &DistributedNode) -> Vec<i32> {
    let mut ids = vec![node.node_id];
    for child in &node.children {
        ids.extend(node_ids(child));
    }
    ids
}

fn node_tuple_ids(node: &DistributedNode) -> Vec<i32> {
    let mut ids = node.tuple_ids.clone();
    ids.extend_from_slice(&node.nullable_tuple_ids);
    for child in &node.children {
        ids.extend(node_tuple_ids(child));
    }
    ids
}

fn validate_output_ordinal(
    output_columns: &[crate::sql::analysis::OutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<(), String> {
    if output_columns.get(ordinal).is_none() {
        return Err(format!(
            "Iceberg change-stream {label} output ordinal {ordinal} is out of range"
        ));
    }
    Ok(())
}

fn validate_output_ordinals(
    output_columns: &[crate::sql::analysis::OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<(), String> {
    for ordinal in ordinals {
        validate_output_ordinal(output_columns, *ordinal, label)?;
    }
    Ok(())
}

fn output_columns_by_ordinals(
    output_columns: &[crate::sql::analysis::OutputColumn],
    ordinals: &[usize],
) -> Result<Vec<crate::sql::analysis::OutputColumn>, String> {
    ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            output_columns.get(ordinal).cloned().ok_or_else(|| {
                format!("Iceberg change-stream branch output ordinal {ordinal} is out of range")
            })
        })
        .collect()
}

fn partition_type_for_ordinals(ordinals: &[usize]) -> partitions::TPartitionType {
    if ordinals.is_empty() {
        partitions::TPartitionType::UNPARTITIONED
    } else {
        partitions::TPartitionType::HASH_PARTITIONED
    }
}

fn stream_kind_for_partition_type(
    partition_type: partitions::TPartitionType,
) -> FragmentStreamKind {
    match partition_type {
        partitions::TPartitionType::HASH_PARTITIONED
        | partitions::TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED => {
            FragmentStreamKind::Partitioned
        }
        partitions::TPartitionType::UNPARTITIONED => FragmentStreamKind::Gather,
        _ => FragmentStreamKind::Other,
    }
}

fn tdata_partition_placeholder(
    partition_type: partitions::TPartitionType,
) -> partitions::TDataPartition {
    partitions::TDataPartition::new(
        partition_type,
        None::<Vec<crate::thrift::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    )
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::ChangeStreamBranchKind;
    use crate::sql::planner::{
        ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec, DataPartition, DataSink,
        DistributedNode, DistributedPayload, DistributedPlan, IcebergWriteFragmentSink,
        IcebergWriteInputBinding, PlanFragment,
    };
    use crate::sql::planner::{PhysicalPlanStats, PlannerConfidence};

    use super::{with_iceberg_change_stream_write, with_iceberg_write_sink};

    #[test]
    fn with_iceberg_write_sink_replaces_root_result_sink() {
        let plan = single_fragment_plan_for_test();
        let sink = IcebergWriteFragmentSink {
            descriptor_database: "test_db".to_string(),
            spec: crate::sql::planner::write_sink::test_support::simple_sink_spec(),
            input: IcebergWriteInputBinding::RootOutputByOrdinal,
        };

        let planned = with_iceberg_write_sink(plan, sink).expect("plan write sink");

        let root = planned
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == planned.root_fragment_id)
            .expect("root fragment");
        assert!(matches!(root.sink, DataSink::IcebergWrite(_)));
    }

    #[test]
    fn with_iceberg_write_sink_rejects_arity_mismatch() {
        let plan = single_fragment_plan_for_test_with_columns(vec![
            ("a", DataType::Int32),
            ("b", DataType::Int32),
        ]);
        let sink = IcebergWriteFragmentSink {
            descriptor_database: "test_db".to_string(),
            spec: crate::sql::planner::write_sink::test_support::simple_sink_spec(),
            input: IcebergWriteInputBinding::OutputOrdinals(vec![0, 1]),
        };

        let err = with_iceberg_write_sink(plan, sink).expect_err("arity mismatch");

        assert!(err.contains(
            "Iceberg write sink input column count 2 does not match target column count 1"
        ));
    }

    #[test]
    fn with_iceberg_write_sink_rejects_out_of_range_output_ordinal() {
        let plan = single_fragment_plan_for_test();
        let sink = IcebergWriteFragmentSink {
            descriptor_database: "test_db".to_string(),
            spec: crate::sql::planner::write_sink::test_support::simple_sink_spec(),
            input: IcebergWriteInputBinding::OutputOrdinals(vec![7]),
        };

        let err = with_iceberg_write_sink(plan, sink).expect_err("out-of-range ordinal");

        assert!(err.contains("Iceberg write sink output ordinal 7 is out of range"));
    }

    #[test]
    fn change_stream_expander_adds_router_and_writer_fragments() {
        let plan = single_fragment_plan_for_test_with_columns(vec![
            ("op", DataType::Int32),
            ("route", DataType::Int32),
            ("delete_id", DataType::Int32),
            ("reuse_id", DataType::Int32),
        ]);
        let mut delete_branch = ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]);
        delete_branch.output_partition_ordinals = vec![2];
        let reuse_branch = ChangeStreamWriteBranchSpec::reuse_data_for_test(vec![3]);
        let dag =
            ChangeStreamWriteDagSpec::for_test(Some(0), Some(1), vec![delete_branch, reuse_branch]);

        let planned =
            with_iceberg_change_stream_write(plan, "test_db", dag).expect("plan change stream");

        assert_eq!(planned.distributed_plan.fragments.len(), 3);
        let root = planned
            .distributed_plan
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == planned.distributed_plan.root_fragment_id)
            .expect("root fragment");
        let DataSink::IcebergChangeStreamRouter(router) = &root.sink else {
            panic!("expected router sink");
        };
        assert_eq!(router.group_id, 0);
        assert_eq!(router.change_op_output_ordinal, 0);
        assert_eq!(router.data_route_output_ordinal, Some(1));
        assert_eq!(router.branches.len(), 2);
        assert_eq!(router.branches[0].output_ordinals, vec![2]);
        assert_eq!(router.branches[0].output_partition_ordinals, vec![2]);

        assert_eq!(planned.distributed_plan.edges.len(), 2);
        let first_edge = &planned.distributed_plan.edges[0];
        assert_eq!(first_edge.source_fragment_id, 0);
        assert_eq!(first_edge.target_fragment_id, 1);
        assert_eq!(
            first_edge.stream_kind,
            crate::sql::codegen::FragmentStreamKind::Partitioned
        );
        assert_eq!(
            first_edge.output_partition.type_,
            crate::thrift::partitions::TPartitionType::HASH_PARTITIONED
        );
        assert!(matches!(
            first_edge.edge_kind,
            crate::sql::codegen::FragmentEdgeKind::IcebergChangeStreamRouter {
                branch_kind: ChangeStreamBranchKind::DeleteDv,
                ..
            }
        ));

        let writer = planned
            .distributed_plan
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == first_edge.target_fragment_id)
            .expect("writer fragment");
        assert!(matches!(writer.sink, DataSink::IcebergWrite(_)));
        assert_eq!(writer.output_columns.len(), 1);
        assert_eq!(writer.output_columns[0].name, "delete_id");
        assert_eq!(planned.topology.writer_branches.len(), 2);
        assert_eq!(
            planned.topology.writer_branches[0]
                .sink_spec
                .target_table_id,
            crate::sql::planner::synthetic_iceberg_write_table_id()
        );
        assert_eq!(
            planned.topology.writer_branches[1]
                .sink_spec
                .target_table_id,
            crate::sql::planner::synthetic_iceberg_write_table_id() - 1
        );
    }

    #[test]
    fn change_stream_expander_rejects_missing_change_op_ordinal() {
        let plan = single_fragment_plan_for_test();
        let dag = ChangeStreamWriteDagSpec::for_test(
            None,
            None,
            vec![ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![0])],
        );

        let err =
            with_iceberg_change_stream_write(plan, "test_db", dag).expect_err("missing change_op");

        assert!(err.contains("requires change_op_output_ordinal"));
    }

    fn single_fragment_plan_for_test() -> DistributedPlan {
        single_fragment_plan_for_test_with_columns(vec![("id", DataType::Int32)])
    }

    fn single_fragment_plan_for_test_with_columns(
        columns: Vec<(&str, DataType)>,
    ) -> DistributedPlan {
        let output_columns = columns
            .into_iter()
            .enumerate()
            .map(|(idx, (name, data_type))| OutputColumn {
                column_id: ColumnId::new_for_test((idx + 1) as u32),
                name: name.to_string(),
                data_type,
                nullable: false,
                is_internal: false,
            })
            .collect::<Vec<_>>();
        DistributedPlan {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 0,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    build_runtime_filters: vec![],
                    probe_runtime_filters: vec![],
                    children: vec![],
                    stats: stats(),
                    payload: DistributedPayload::Physical(
                        crate::sql::planner::plan::PhysicalPlanKind::Values(
                            crate::sql::planner::plan::PlanValuesNode {
                                rows: vec![],
                                columns: output_columns.clone(),
                            },
                        ),
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
            edges: Vec::new(),
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }
}
