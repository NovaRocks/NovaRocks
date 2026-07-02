use crate::sql::analysis::OutputColumn as AnalysisOutputColumn;
use crate::sql::codegen::expr_compiler;
use crate::sql::codegen::resolve::{ColumnBinding, ExprScope};
use crate::sql::common::ChangeStreamBranchKind;
use crate::thrift::data_sinks;
use crate::thrift::exprs;
use crate::thrift::partitions;

pub(in crate::sql::codegen) fn branch_kind_to_thrift(
    kind: ChangeStreamBranchKind,
) -> data_sinks::TIcebergChangeStreamRouterBranchKind {
    match kind {
        ChangeStreamBranchKind::DeleteDv => {
            data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV
        }
        ChangeStreamBranchKind::ReuseData => {
            data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA
        }
        ChangeStreamBranchKind::FreshData => {
            data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA
        }
    }
}

pub(in crate::sql::codegen) fn build_router_sink_thrift(
    sink: &crate::sql::planner::IcebergChangeStreamRouterSink,
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
) -> Result<data_sinks::TDataSink, String> {
    let change_op_slot_id = slot_id_for_ordinal(
        scope,
        output_columns,
        sink.change_op_output_ordinal,
        "change_op",
    )?;
    let data_route_slot_id = sink
        .data_route_output_ordinal
        .map(|ordinal| slot_id_for_ordinal(scope, output_columns, ordinal, "data_route"))
        .transpose()?;

    let mut branches = Vec::with_capacity(sink.branches.len());
    for branch in &sink.branches {
        let label = format!("branch {:?} output", branch.branch_kind);
        let output_slots =
            output_slot_ids_for_ordinals(scope, output_columns, &branch.output_ordinals, &label)?;
        let partition = output_partition_for_ordinals(
            scope,
            output_columns,
            &branch.output_partition_ordinals,
            &format!("branch {:?} partition", branch.branch_kind),
        )?;
        let stream_sink = data_sinks::TDataStreamSink::new(
            -1,
            partition,
            None::<bool>,
            None::<bool>,
            None::<i32>,
            Some(output_slots),
            None::<i64>,
        );
        branches.push(data_sinks::TIcebergChangeStreamRouterBranch::new(
            branch.branch_id,
            branch_kind_to_thrift(branch.branch_kind),
            stream_sink,
            Vec::new(),
        ));
    }

    Ok(data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK,
        None::<data_sinks::TDataStreamSink>,
        None::<data_sinks::TResultSink>,
        None::<data_sinks::TMysqlTableSink>,
        None::<data_sinks::TExportSink>,
        None::<data_sinks::TOlapTableSink>,
        None::<data_sinks::TMemoryScratchSink>,
        None::<data_sinks::TMultiCastDataStreamSink>,
        None::<data_sinks::TSchemaTableSink>,
        None::<data_sinks::TIcebergTableSink>,
        None::<data_sinks::THiveTableSink>,
        None::<data_sinks::TTableFunctionTableSink>,
        None::<data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<data_sinks::TDataSink>>>,
        None::<i64>,
        None::<data_sinks::TSplitDataStreamSink>,
        Some(data_sinks::TIcebergChangeStreamRouterSink::new(
            change_op_slot_id,
            data_route_slot_id,
            branches,
        )),
    ))
}

pub(in crate::sql::codegen) fn output_partition_for_ordinals(
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<partitions::TDataPartition, String> {
    if ordinals.is_empty() {
        return Ok(partitions::TDataPartition::new(
            partitions::TPartitionType::UNPARTITIONED,
            None::<Vec<exprs::TExpr>>,
            None::<Vec<partitions::TRangePartition>>,
            None::<Vec<partitions::TBucketProperty>>,
        ));
    }

    let exprs = ordinals
        .iter()
        .copied()
        .map(|ordinal| slot_ref_expr_for_ordinal(scope, output_columns, ordinal, label))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(partitions::TDataPartition::new(
        partitions::TPartitionType::HASH_PARTITIONED,
        Some(exprs),
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    ))
}

pub(in crate::sql::codegen) fn output_slot_ids_for_ordinals(
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<Vec<i32>, String> {
    ordinals
        .iter()
        .copied()
        .map(|ordinal| slot_id_for_ordinal(scope, output_columns, ordinal, label))
        .collect()
}

fn slot_ref_expr_for_ordinal(
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<exprs::TExpr, String> {
    let binding = binding_for_ordinal(scope, output_columns, ordinal, label)?;
    let type_desc = expr_compiler::binding_type_desc(binding)?;
    Ok(expr_compiler::build_slot_ref_texpr(
        binding.slot_id,
        binding.tuple_id,
        type_desc,
    ))
}

fn slot_id_for_ordinal(
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<i32, String> {
    Ok(binding_for_ordinal(scope, output_columns, ordinal, label)?.slot_id)
}

fn binding_for_ordinal<'a>(
    scope: &'a ExprScope,
    output_columns: &'a [AnalysisOutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<&'a ColumnBinding, String> {
    let column = output_columns.get(ordinal).ok_or_else(|| {
        format!("Iceberg change-stream {label} output ordinal {ordinal} is out of range")
    })?;
    scope.resolve_by_id(column.column_id).ok_or_else(|| {
        format!(
            "Iceberg change-stream {label} output ordinal {ordinal} column `{}` id={} has no materialized slot",
            column.name, column.column_id.0
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_thrift_maps_known_branch_kinds() {
        assert_eq!(
            branch_kind_to_thrift(ChangeStreamBranchKind::DeleteDv),
            data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV
        );
        assert_eq!(
            branch_kind_to_thrift(ChangeStreamBranchKind::ReuseData),
            data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA
        );
        assert_eq!(
            branch_kind_to_thrift(ChangeStreamBranchKind::FreshData),
            data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA
        );
    }
}
