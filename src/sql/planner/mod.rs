use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::common::types::UniqueId;
use crate::descriptors;
use crate::internal_service;
use crate::opcodes;
use crate::partitions;
use crate::plan_nodes;
use crate::runtime::result_buffer::{self, TryFetchResult};
use crate::sql::analyzer::{BoundQuery, QuerySource};
use crate::sql::optimizer::RelQueryPlan;
use crate::sql::{CompareOp, Literal};
use crate::types;

const STANDALONE_SCAN_TUPLE_ID: types::TTupleId = 1;
const STANDALONE_PROJECT_TUPLE_ID: types::TTupleId = 2;
const STANDALONE_PROJECT_NODE_ID: types::TPlanNodeId = 1;
const STANDALONE_SCAN_NODE_ID: types::TPlanNodeId = 2;

static NEXT_REQUEST_ID: AtomicI64 = AtomicI64::new(10_000);

#[derive(Clone, Debug)]
pub(crate) struct PlannedQueryColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedQuery {
    pub(crate) request: internal_service::TExecPlanFragmentParams,
    pub(crate) columns: Vec<PlannedQueryColumn>,
    pub(crate) order_by: Vec<crate::sql::ast::OrderByExpr>,
}

pub(crate) fn build_local_query_plan_fragment(plan: &RelQueryPlan) -> Result<PlannedQuery, String> {
    let QuerySource::Local { table, resolved } = &plan.source else {
        return Err("only local standalone queries can be lowered to thrift fragment".to_string());
    };

    let desc_tbl = build_descriptor_table(&plan.bound)?;
    let fragment = build_fragment(table, &plan.bound)?;
    let params = build_exec_params(table, STANDALONE_SCAN_NODE_ID)?;
    let request = internal_service::TExecPlanFragmentParams::new(
        internal_service::InternalServiceVersion::V1,
        Some(fragment),
        Some(desc_tbl),
        Some(params),
        None::<types::TNetworkAddress>,
        None::<i32>,
        None::<internal_service::TQueryGlobals>,
        None::<internal_service::TQueryOptions>,
        Some(false),
        None::<types::TResourceInfo>,
        None::<String>,
        Some(resolved.database.clone()),
        None::<i64>,
        None::<internal_service::TLoadErrorHubInfo>,
        Some(true),
        Some(1),
        None::<BTreeMap<types::TPlanNodeId, i32>>,
        None::<crate::work_group::TWorkGroup>,
        None::<bool>,
        None::<i32>,
        None::<bool>,
        None::<bool>,
        None::<internal_service::TAdaptiveDopParam>,
        None::<i32>,
        None::<internal_service::TPredicateTreeParams>,
        None::<Vec<i32>>,
    );
    Ok(PlannedQuery {
        request,
        columns: plan
            .bound
            .output_columns
            .iter()
            .map(|column| PlannedQueryColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
            .collect(),
        order_by: plan.order_by.clone(),
    })
}

pub(crate) fn collect_query_result(
    finst_id: UniqueId,
    columns: &[PlannedQueryColumn],
) -> Result<RecordBatch, String> {
    let mut rows = Vec::new();
    loop {
        match result_buffer::try_fetch(finst_id) {
            TryFetchResult::Ready(fetch) if fetch.eos => break,
            TryFetchResult::Ready(fetch) => rows.extend(fetch.result_batch.rows),
            TryFetchResult::NotReady => std::thread::yield_now(),
            TryFetchResult::Error(err) => return Err(err.message),
        }
    }
    decode_mysql_text_rows(columns, &rows)
}

fn decode_mysql_text_rows(
    columns: &[PlannedQueryColumn],
    rows: &[Vec<u8>],
) -> Result<RecordBatch, String> {
    let mut decoded_rows = Vec::with_capacity(rows.len());
    for row in rows {
        decoded_rows.push(parse_lenenc_row(row, columns.len())?);
    }

    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    ));

    let arrays = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| decode_column_array(column, &decoded_rows, idx))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build standalone result batch failed: {e}"))
}

fn decode_column_array(
    column: &PlannedQueryColumn,
    rows: &[Vec<Option<Vec<u8>>>],
    column_idx: usize,
) -> Result<ArrayRef, String> {
    match &column.data_type {
        DataType::Boolean => Ok(std::sync::Arc::new(BooleanArray::from(
            rows.iter()
                .map(|row| {
                    row[column_idx]
                        .as_ref()
                        .map(|bytes| bytes.as_slice() == b"1")
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int8 => Ok(std::sync::Arc::new(Int8Array::from(parse_numeric_column(
            rows, column_idx, "TINYINT",
        )?))),
        DataType::Int16 => Ok(std::sync::Arc::new(Int16Array::from(parse_numeric_column(
            rows, column_idx, "SMALLINT",
        )?))),
        DataType::Int32 => Ok(std::sync::Arc::new(Int32Array::from(parse_numeric_column(
            rows, column_idx, "INT",
        )?))),
        DataType::Int64 => Ok(std::sync::Arc::new(Int64Array::from(parse_numeric_column(
            rows, column_idx, "BIGINT",
        )?))),
        DataType::Float32 => Ok(std::sync::Arc::new(Float32Array::from(
            parse_numeric_column(rows, column_idx, "FLOAT")?,
        ))),
        DataType::Float64 => Ok(std::sync::Arc::new(Float64Array::from(
            parse_numeric_column(rows, column_idx, "DOUBLE")?,
        ))),
        DataType::Utf8 => Ok(std::sync::Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    row[column_idx]
                        .as_ref()
                        .map(|bytes| String::from_utf8(bytes.clone()))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("decode UTF8 result failed: {e}"))?,
        ))),
        DataType::LargeUtf8 => Ok(std::sync::Arc::new(LargeStringArray::from(
            rows.iter()
                .map(|row| {
                    row[column_idx]
                        .as_ref()
                        .map(|bytes| String::from_utf8(bytes.clone()))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("decode UTF8 result failed: {e}"))?,
        ))),
        DataType::Binary => Ok(std::sync::Arc::new(BinaryArray::from(
            rows.iter()
                .map(|row| row[column_idx].as_deref())
                .collect::<Vec<_>>(),
        ))),
        DataType::Date32 => Ok(std::sync::Arc::new(Date32Array::from(
            rows.iter()
                .map(|row| {
                    row[column_idx]
                        .as_ref()
                        .map(|bytes| parse_date32(bytes))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => Ok(std::sync::Arc::new(TimestampSecondArray::from(
                parse_timestamp_column(rows, column_idx, TimeUnit::Second)?,
            ))),
            TimeUnit::Millisecond => Ok(std::sync::Arc::new(TimestampMillisecondArray::from(
                parse_timestamp_column(rows, column_idx, TimeUnit::Millisecond)?,
            ))),
            TimeUnit::Microsecond => Ok(std::sync::Arc::new(TimestampMicrosecondArray::from(
                parse_timestamp_column(rows, column_idx, TimeUnit::Microsecond)?,
            ))),
            TimeUnit::Nanosecond => Ok(std::sync::Arc::new(TimestampNanosecondArray::from(
                parse_timestamp_column(rows, column_idx, TimeUnit::Nanosecond)?,
            ))),
        },
        other => Err(format!(
            "standalone thrift result decoder does not support {:?}",
            other
        )),
    }
}

fn parse_numeric_column<T>(
    rows: &[Vec<Option<Vec<u8>>>],
    column_idx: usize,
    type_name: &str,
) -> Result<Vec<Option<T>>, String>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    rows.iter()
        .map(|row| {
            row[column_idx]
                .as_ref()
                .map(|bytes| {
                    std::str::from_utf8(bytes)
                        .map_err(|e| format!("decode {type_name} text failed: {e}"))?
                        .parse::<T>()
                        .map_err(|e| format!("parse {type_name} value failed: {e}"))
                })
                .transpose()
        })
        .collect()
}

fn parse_date32(bytes: &[u8]) -> Result<i32, String> {
    let text = std::str::from_utf8(bytes).map_err(|e| format!("decode DATE text failed: {e}"))?;
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|e| format!("parse DATE value failed: {e}"))?;
    Ok(date.num_days_from_ce() - 719_163)
}

fn parse_timestamp_column(
    rows: &[Vec<Option<Vec<u8>>>],
    column_idx: usize,
    unit: TimeUnit,
) -> Result<Vec<Option<i64>>, String> {
    rows.iter()
        .map(|row| {
            row[column_idx]
                .as_ref()
                .map(|bytes| parse_timestamp(bytes, unit))
                .transpose()
        })
        .collect()
}

fn parse_timestamp(bytes: &[u8], unit: TimeUnit) -> Result<i64, String> {
    let text =
        std::str::from_utf8(bytes).map_err(|e| format!("decode DATETIME text failed: {e}"))?;
    let value = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| format!("parse DATETIME value failed: {e}"))?;
    let utc = value.and_utc();
    let nanos = utc
        .timestamp_nanos_opt()
        .ok_or_else(|| "timestamp overflows i64".to_string())?;
    Ok(match unit {
        TimeUnit::Second => utc.timestamp(),
        TimeUnit::Millisecond => utc.timestamp_millis(),
        TimeUnit::Microsecond => utc.timestamp_micros(),
        TimeUnit::Nanosecond => nanos,
    })
}

fn parse_lenenc_row(row: &[u8], expected_fields: usize) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut fields = Vec::with_capacity(expected_fields);
    let mut idx = 0usize;
    while idx < row.len() && fields.len() < expected_fields {
        if row[idx] == 0xFB {
            fields.push(None);
            idx += 1;
            continue;
        }
        let len = parse_lenenc_int(row, &mut idx)?;
        let end = idx
            .checked_add(len)
            .ok_or_else(|| "lenenc row length overflow".to_string())?;
        if end > row.len() {
            return Err("lenenc row is truncated".to_string());
        }
        fields.push(Some(row[idx..end].to_vec()));
        idx = end;
    }
    if fields.len() != expected_fields {
        return Err(format!(
            "lenenc row field count mismatch: expected {}, got {}",
            expected_fields,
            fields.len()
        ));
    }
    Ok(fields)
}

fn parse_lenenc_int(row: &[u8], idx: &mut usize) -> Result<usize, String> {
    let Some(&first) = row.get(*idx) else {
        return Err("lenenc row is truncated".to_string());
    };
    *idx += 1;
    let value = match first {
        0x00..=0xFA => u64::from(first),
        0xFC => read_fixed_int(row, idx, 2)?,
        0xFD => read_fixed_int(row, idx, 3)?,
        0xFE => read_fixed_int(row, idx, 8)?,
        0xFB => return Err("unexpected NULL marker while parsing lenenc integer".to_string()),
        0xFF => return Err("invalid lenenc integer prefix".to_string()),
    };
    usize::try_from(value).map_err(|_| "lenenc integer overflows usize".to_string())
}

fn read_fixed_int(row: &[u8], idx: &mut usize, width: usize) -> Result<u64, String> {
    let end = idx
        .checked_add(width)
        .ok_or_else(|| "lenenc integer overflow".to_string())?;
    let bytes = row
        .get(*idx..end)
        .ok_or_else(|| "lenenc integer is truncated".to_string())?;
    *idx = end;
    Ok(bytes.iter().enumerate().fold(0u64, |acc, (shift, byte)| {
        acc | (u64::from(*byte) << (shift * 8))
    }))
}

fn build_fragment(
    table: &crate::standalone::catalog::TableDef,
    bound: &BoundQuery,
) -> Result<crate::planner::TPlanFragment, String> {
    let plan = build_plan(table, bound)?;
    let result_sink = crate::data_sinks::TResultSink::new(
        Some(crate::data_sinks::TResultSinkType::MYSQL_PROTOCAL),
        None::<crate::data_sinks::TResultFileSinkOptions>,
        None::<crate::data_sinks::TResultSinkFormatType>,
        Some(false),
        Some(
            bound
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>(),
        ),
    );
    let output_sink = crate::data_sinks::TDataSink::new(
        crate::data_sinks::TDataSinkType::RESULT_SINK,
        None::<crate::data_sinks::TDataStreamSink>,
        Some(result_sink),
        None::<crate::data_sinks::TMysqlTableSink>,
        None::<crate::data_sinks::TExportSink>,
        None::<crate::data_sinks::TOlapTableSink>,
        None::<crate::data_sinks::TMemoryScratchSink>,
        None::<crate::data_sinks::TMultiCastDataStreamSink>,
        None::<crate::data_sinks::TSchemaTableSink>,
        None::<crate::data_sinks::TIcebergTableSink>,
        None::<crate::data_sinks::THiveTableSink>,
        None::<crate::data_sinks::TTableFunctionTableSink>,
        None::<crate::data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<crate::data_sinks::TDataSink>>>,
        None::<i64>,
        None::<crate::data_sinks::TSplitDataStreamSink>,
    );
    Ok(crate::planner::TPlanFragment::new(
        Some(plan),
        None::<Vec<crate::exprs::TExpr>>,
        Some(output_sink),
        partitions::TDataPartition::new(
            partitions::TPartitionType::UNPARTITIONED,
            None::<Vec<crate::exprs::TExpr>>,
            None::<Vec<partitions::TRangePartition>>,
            None::<Vec<partitions::TBucketProperty>>,
        ),
        None::<i64>,
        None::<i64>,
        None::<Vec<crate::data::TGlobalDict>>,
        None::<Vec<crate::data::TGlobalDict>>,
        None::<crate::planner::TCacheParam>,
        None::<BTreeMap<i32, crate::exprs::TExpr>>,
        None::<crate::planner::TGroupExecutionParam>,
    ))
}

fn build_plan(
    table: &crate::standalone::catalog::TableDef,
    bound: &BoundQuery,
) -> Result<plan_nodes::TPlan, String> {
    let scan_node = build_scan_node(table, bound)?;
    if !bound.needs_project {
        return Ok(plan_nodes::TPlan::new(vec![scan_node]));
    }
    let project_node = build_project_node(bound)?;
    Ok(plan_nodes::TPlan::new(vec![project_node, scan_node]))
}

fn build_scan_node(
    table: &crate::standalone::catalog::TableDef,
    bound: &BoundQuery,
) -> Result<plan_nodes::TPlanNode, String> {
    Ok(plan_nodes::TPlanNode {
        node_id: STANDALONE_SCAN_NODE_ID,
        node_type: plan_nodes::TPlanNodeType::HDFS_SCAN_NODE,
        num_children: 0,
        limit: -1,
        row_tuples: vec![STANDALONE_SCAN_TUPLE_ID],
        nullable_tuples: vec![],
        conjuncts: build_scan_conjuncts(bound)?,
        compact_data: true,
        common: None,
        hash_join_node: None,
        agg_node: None,
        sort_node: None,
        merge_node: None,
        exchange_node: None,
        mysql_scan_node: None,
        olap_scan_node: None,
        file_scan_node: None,
        schema_scan_node: None,
        meta_scan_node: None,
        analytic_node: None,
        union_node: None,
        resource_profile: None,
        es_scan_node: None,
        repeat_node: None,
        assert_num_rows_node: None,
        intersect_node: None,
        except_node: None,
        merge_join_node: None,
        raw_values_node: None,
        use_vectorized: None,
        hdfs_scan_node: Some(plan_nodes::THdfsScanNode::new(
            Some(STANDALONE_SCAN_TUPLE_ID),
            None::<BTreeMap<types::TTupleId, Vec<crate::exprs::TExpr>>>,
            None::<Vec<crate::exprs::TExpr>>,
            None::<types::TTupleId>,
            None::<BTreeMap<types::TSlotId, Vec<i32>>>,
            None::<Vec<crate::exprs::TExpr>>,
            Some(
                table
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>(),
            ),
            Some(table.name.clone()),
            None::<String>,
            None::<String>,
            None::<String>,
            Some(true),
            None::<crate::cloud_configuration::TCloudConfiguration>,
            None::<bool>,
            None::<bool>,
            None::<bool>,
            None::<types::TTupleId>,
            None::<String>,
            None::<String>,
            None::<bool>,
            None::<String>,
            None::<crate::data_cache::TDataCacheOptions>,
            None::<Vec<types::TSlotId>>,
            None::<bool>,
            None::<Vec<partitions::TBucketProperty>>,
        )),
        project_node: None,
        table_function_node: None,
        probe_runtime_filters: None,
        decode_node: None,
        local_rf_waiting_set: None,
        filter_null_value_columns: None,
        need_create_tuple_columns: None,
        jdbc_scan_node: None,
        connector_scan_node: None,
        cross_join_node: None,
        lake_scan_node: None,
        nestloop_join_node: None,
        starrocks_scan_node: None,
        stream_scan_node: None,
        stream_join_node: None,
        stream_agg_node: None,
        select_node: None,
        fetch_node: None,
        look_up_node: None,
        benchmark_scan_node: None,
    })
}

fn build_project_node(bound: &BoundQuery) -> Result<plan_nodes::TPlanNode, String> {
    let mut slot_map = BTreeMap::new();
    for output in &bound.output_columns {
        let output_slot_id = output
            .output_slot_id
            .ok_or_else(|| "project node requires output slot ids".to_string())?;
        let scan_column = &bound.scan_columns[output.scan_column_index];
        slot_map.insert(
            output_slot_id,
            build_slot_ref_expr(
                scan_column.scan_slot_id,
                STANDALONE_SCAN_TUPLE_ID,
                arrow_type_to_type_desc(&scan_column.data_type)?,
            ),
        );
    }
    Ok(plan_nodes::TPlanNode {
        node_id: STANDALONE_PROJECT_NODE_ID,
        node_type: plan_nodes::TPlanNodeType::PROJECT_NODE,
        num_children: 1,
        limit: -1,
        row_tuples: vec![STANDALONE_PROJECT_TUPLE_ID],
        nullable_tuples: vec![],
        conjuncts: None,
        compact_data: true,
        common: None,
        hash_join_node: None,
        agg_node: None,
        sort_node: None,
        merge_node: None,
        exchange_node: None,
        mysql_scan_node: None,
        olap_scan_node: None,
        file_scan_node: None,
        schema_scan_node: None,
        meta_scan_node: None,
        analytic_node: None,
        union_node: None,
        resource_profile: None,
        es_scan_node: None,
        repeat_node: None,
        assert_num_rows_node: None,
        intersect_node: None,
        except_node: None,
        merge_join_node: None,
        raw_values_node: None,
        use_vectorized: None,
        hdfs_scan_node: None,
        project_node: Some(plan_nodes::TProjectNode::new(
            Some(slot_map),
            None::<BTreeMap<types::TSlotId, crate::exprs::TExpr>>,
        )),
        table_function_node: None,
        probe_runtime_filters: None,
        decode_node: None,
        local_rf_waiting_set: None,
        filter_null_value_columns: None,
        need_create_tuple_columns: None,
        jdbc_scan_node: None,
        connector_scan_node: None,
        cross_join_node: None,
        lake_scan_node: None,
        nestloop_join_node: None,
        starrocks_scan_node: None,
        stream_scan_node: None,
        stream_join_node: None,
        stream_agg_node: None,
        select_node: None,
        fetch_node: None,
        look_up_node: None,
        benchmark_scan_node: None,
    })
}

fn build_scan_conjuncts(bound: &BoundQuery) -> Result<Option<Vec<crate::exprs::TExpr>>, String> {
    let Some(predicate) = bound.predicate.as_ref() else {
        return Ok(None);
    };
    let scan_column = &bound.scan_columns[predicate.scan_column_index];
    let compare_type = arrow_type_to_type_desc(&scan_column.data_type)?;
    let root = crate::exprs::TExprNode {
        node_type: crate::exprs::TExprNodeType::BINARY_PRED,
        type_: crate::lower::thrift::type_lowering::scalar_type_desc(
            types::TPrimitiveType::BOOLEAN,
        ),
        opcode: Some(compare_op_to_opcode(predicate.op)),
        num_children: 2,
        child_type_desc: Some(compare_type.clone()),
        ..default_expr_node()
    };
    let left = slot_ref_expr_node(
        scan_column.scan_slot_id,
        STANDALONE_SCAN_TUPLE_ID,
        compare_type.clone(),
    );
    let right = literal_expr_node(&predicate.literal, &scan_column.data_type)?;
    Ok(Some(vec![crate::exprs::TExpr::new(vec![
        root, left, right,
    ])]))
}

fn compare_op_to_opcode(op: CompareOp) -> opcodes::TExprOpcode {
    match op {
        CompareOp::Eq => opcodes::TExprOpcode::EQ,
        CompareOp::Ne => opcodes::TExprOpcode::NE,
        CompareOp::Lt => opcodes::TExprOpcode::LT,
        CompareOp::Le => opcodes::TExprOpcode::LE,
        CompareOp::Gt => opcodes::TExprOpcode::GT,
        CompareOp::Ge => opcodes::TExprOpcode::GE,
    }
}

fn build_slot_ref_expr(
    slot_id: types::TSlotId,
    tuple_id: types::TTupleId,
    type_desc: types::TTypeDesc,
) -> crate::exprs::TExpr {
    crate::exprs::TExpr::new(vec![slot_ref_expr_node(slot_id, tuple_id, type_desc)])
}

fn slot_ref_expr_node(
    slot_id: types::TSlotId,
    tuple_id: types::TTupleId,
    type_desc: types::TTypeDesc,
) -> crate::exprs::TExprNode {
    crate::exprs::TExprNode {
        node_type: crate::exprs::TExprNodeType::SLOT_REF,
        type_: type_desc,
        num_children: 0,
        slot_ref: Some(crate::exprs::TSlotRef { slot_id, tuple_id }),
        ..default_expr_node()
    }
}

fn literal_expr_node(
    literal: &Literal,
    target_type: &DataType,
) -> Result<crate::exprs::TExprNode, String> {
    let node = match literal {
        Literal::Null => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::NULL_LITERAL,
            type_: arrow_type_to_type_desc(target_type)?,
            num_children: 0,
            ..default_expr_node()
        },
        Literal::Bool(value) => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::BOOL_LITERAL,
            type_: crate::lower::thrift::type_lowering::scalar_type_desc(
                types::TPrimitiveType::BOOLEAN,
            ),
            num_children: 0,
            bool_literal: Some(crate::exprs::TBoolLiteral { value: *value }),
            ..default_expr_node()
        },
        Literal::Int(value) => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::INT_LITERAL,
            type_: int_literal_type_desc(target_type),
            num_children: 0,
            int_literal: Some(crate::exprs::TIntLiteral { value: *value }),
            ..default_expr_node()
        },
        Literal::Float(value) => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::FLOAT_LITERAL,
            type_: float_literal_type_desc(target_type),
            num_children: 0,
            float_literal: Some(crate::exprs::TFloatLiteral {
                value: thrift::OrderedFloat::from(*value),
            }),
            ..default_expr_node()
        },
        Literal::String(value) => crate::exprs::TExprNode {
            node_type: crate::exprs::TExprNodeType::STRING_LITERAL,
            type_: string_literal_type_desc(target_type),
            num_children: 0,
            string_literal: Some(crate::exprs::TStringLiteral {
                value: value.clone(),
            }),
            ..default_expr_node()
        },
        Literal::Date(value) => match target_type {
            DataType::Date32 => crate::exprs::TExprNode {
                node_type: crate::exprs::TExprNodeType::DATE_LITERAL,
                type_: crate::lower::thrift::type_lowering::scalar_type_desc(
                    types::TPrimitiveType::DATE,
                ),
                num_children: 0,
                date_literal: Some(crate::exprs::TDateLiteral {
                    value: value.clone(),
                }),
                ..default_expr_node()
            },
            DataType::Timestamp(_, _) => crate::exprs::TExprNode {
                node_type: crate::exprs::TExprNodeType::STRING_LITERAL,
                type_: crate::lower::thrift::type_lowering::scalar_type_desc(
                    types::TPrimitiveType::VARCHAR,
                ),
                num_children: 0,
                string_literal: Some(crate::exprs::TStringLiteral {
                    value: value.clone(),
                }),
                ..default_expr_node()
            },
            other => {
                return Err(format!(
                    "date literal is not supported for column type {:?}",
                    other
                ));
            }
        },
    };
    Ok(node)
}

fn int_literal_type_desc(target_type: &DataType) -> types::TTypeDesc {
    match target_type {
        DataType::Int8 => {
            crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::TINYINT)
        }
        DataType::Int16 => {
            crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::SMALLINT)
        }
        DataType::Int32 => {
            crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::INT)
        }
        DataType::Int64 => {
            crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::BIGINT)
        }
        _ => crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::BIGINT),
    }
}

fn float_literal_type_desc(target_type: &DataType) -> types::TTypeDesc {
    match target_type {
        DataType::Float32 => {
            crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::FLOAT)
        }
        _ => crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::DOUBLE),
    }
}

fn string_literal_type_desc(target_type: &DataType) -> types::TTypeDesc {
    match target_type {
        DataType::Binary => {
            crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::VARBINARY)
        }
        _ => crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::VARCHAR),
    }
}

fn default_expr_node() -> crate::exprs::TExprNode {
    crate::exprs::TExprNode {
        node_type: crate::exprs::TExprNodeType::INT_LITERAL,
        type_: crate::lower::thrift::type_lowering::scalar_type_desc(types::TPrimitiveType::INT),
        opcode: None,
        num_children: 0,
        agg_expr: None,
        bool_literal: None,
        case_expr: None,
        date_literal: None,
        float_literal: None,
        int_literal: None,
        in_predicate: None,
        is_null_pred: None,
        like_pred: None,
        literal_pred: None,
        slot_ref: None,
        string_literal: None,
        tuple_is_null_pred: None,
        info_func: None,
        decimal_literal: None,
        output_scale: 0,
        fn_call_expr: None,
        large_int_literal: None,
        output_column: None,
        output_type: None,
        vector_opcode: None,
        fn_: None,
        vararg_start_idx: None,
        child_type: None,
        vslot_ref: None,
        used_subfield_names: None,
        binary_literal: None,
        copy_flag: None,
        check_is_out_of_bounds: None,
        use_vectorized: None,
        has_nullable_child: None,
        is_nullable: None,
        child_type_desc: None,
        is_monotonic: None,
        dict_query_expr: None,
        dictionary_get_expr: None,
        is_index_only_filter: None,
        is_nondeterministic: None,
    }
}

fn build_exec_params(
    table: &crate::standalone::catalog::TableDef,
    scan_node_id: types::TPlanNodeId,
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    let crate::standalone::catalog::TableStorage::LocalParquetFile { path } = &table.storage;
    let metadata = std::fs::metadata(path).map_err(|e| format!("stat parquet file failed: {e}"))?;
    let file_len =
        i64::try_from(metadata.len()).map_err(|_| "parquet file is too large".to_string())?;
    let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
        None::<String>,
        Some(0_i64),
        Some(file_len),
        None::<i64>,
        Some(file_len),
        Some(descriptors::THdfsFileFormat::PARQUET),
        None::<descriptors::TTextFileDesc>,
        Some(path.display().to_string()),
        None::<Vec<String>>,
        None::<bool>,
        None::<Vec<plan_nodes::TIcebergDeleteFile>>,
        None::<i64>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<i64>,
        None::<crate::data_cache::TDataCacheOptions>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<BTreeMap<String, String>>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<String>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<plan_nodes::TPaimonDeletionFile>,
        None::<BTreeMap<types::TSlotId, crate::exprs::TExpr>>,
        None::<descriptors::THdfsPartition>,
        None::<types::TTableId>,
        None::<plan_nodes::TDeletionVectorDescriptor>,
        None::<String>,
        None::<i64>,
        None::<bool>,
        None::<BTreeMap<i32, crate::exprs::TExprMinMaxValue>>,
        None::<i32>,
        None::<i64>,
    );
    let scan_range = internal_service::TScanRangeParams::new(
        plan_nodes::TScanRange::new(
            None::<plan_nodes::TInternalScanRange>,
            None::<Vec<u8>>,
            None::<plan_nodes::TBrokerScanRange>,
            None::<plan_nodes::TEsScanRange>,
            Some(hdfs_scan_range),
            None::<plan_nodes::TBinlogScanRange>,
            None::<plan_nodes::TBenchmarkScanRange>,
        ),
        None::<i32>,
        Some(false),
        Some(false),
    );
    let seed = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    Ok(internal_service::TPlanFragmentExecParams::new(
        types::TUniqueId::new(1, seed),
        types::TUniqueId::new(2, seed),
        BTreeMap::from([(scan_node_id, vec![scan_range])]),
        BTreeMap::new(),
        None::<Vec<crate::data_sinks::TPlanFragmentDestination>>,
        None::<i32>,
        None::<i32>,
        None::<bool>,
        None::<bool>,
        None::<crate::runtime_filter::TRuntimeFilterParams>,
        None::<i32>,
        None::<bool>,
        None::<BTreeMap<types::TPlanNodeId, BTreeMap<i32, Vec<internal_service::TScanRangeParams>>>>,
        None::<bool>,
        None::<i32>,
        None::<bool>,
        None::<Vec<internal_service::TExecDebugOption>>,
    ))
}

fn build_descriptor_table(bound: &BoundQuery) -> Result<descriptors::TDescriptorTable, String> {
    let mut slot_descs = Vec::with_capacity(bound.scan_columns.len() + bound.output_columns.len());
    for (idx, column) in bound.scan_columns.iter().enumerate() {
        let slot_type = arrow_type_to_type_desc(&column.data_type)?;
        slot_descs.push(descriptors::TSlotDescriptor::new(
            Some(column.scan_slot_id),
            Some(STANDALONE_SCAN_TUPLE_ID),
            Some(slot_type),
            Some(i32::try_from(idx).map_err(|_| "too many columns".to_string())?),
            Some(0),
            Some(0),
            Some(0),
            Some(column.name.clone()),
            Some(i32::try_from(idx).map_err(|_| "too many columns".to_string())?),
            Some(true),
            Some(true),
            Some(column.nullable),
            None::<i32>,
            None::<String>,
        ));
    }

    for (idx, column) in bound.output_columns.iter().enumerate() {
        let Some(output_slot_id) = column.output_slot_id else {
            continue;
        };
        let slot_type = arrow_type_to_type_desc(&column.data_type)?;
        let output_idx = i32::try_from(idx).map_err(|_| "too many output columns".to_string())?;
        slot_descs.push(descriptors::TSlotDescriptor::new(
            Some(output_slot_id),
            Some(STANDALONE_PROJECT_TUPLE_ID),
            Some(slot_type),
            Some(output_idx),
            Some(0),
            Some(0),
            Some(0),
            Some(column.name.clone()),
            Some(output_idx),
            Some(true),
            Some(true),
            Some(column.nullable),
            None::<i32>,
            None::<String>,
        ));
    }

    let mut tuple_descs = vec![descriptors::TTupleDescriptor::new(
        Some(STANDALONE_SCAN_TUPLE_ID),
        Some(0),
        Some(0),
        None::<types::TTableId>,
        Some(0),
    )];
    if bound.needs_project {
        tuple_descs.push(descriptors::TTupleDescriptor::new(
            Some(STANDALONE_PROJECT_TUPLE_ID),
            Some(0),
            Some(0),
            None::<types::TTableId>,
            Some(0),
        ));
    }
    Ok(descriptors::TDescriptorTable::new(
        Some(slot_descs),
        tuple_descs,
        None::<Vec<descriptors::TTableDescriptor>>,
        None::<bool>,
    ))
}

fn arrow_type_to_type_desc(data_type: &DataType) -> Result<types::TTypeDesc, String> {
    let primitive = match data_type {
        DataType::Boolean => types::TPrimitiveType::BOOLEAN,
        DataType::Int8 => types::TPrimitiveType::TINYINT,
        DataType::Int16 => types::TPrimitiveType::SMALLINT,
        DataType::Int32 => types::TPrimitiveType::INT,
        DataType::Int64 => types::TPrimitiveType::BIGINT,
        DataType::Float32 => types::TPrimitiveType::FLOAT,
        DataType::Float64 => types::TPrimitiveType::DOUBLE,
        DataType::Utf8 | DataType::LargeUtf8 => types::TPrimitiveType::VARCHAR,
        DataType::Binary | DataType::LargeBinary => types::TPrimitiveType::VARBINARY,
        DataType::Date32 => types::TPrimitiveType::DATE,
        DataType::Timestamp(_, _) => types::TPrimitiveType::DATETIME,
        other => {
            return Err(format!(
                "standalone MVP does not support Parquet column type {:?}",
                other
            ));
        }
    };
    Ok(crate::lower::thrift::type_lowering::scalar_type_desc(
        primitive,
    ))
}
