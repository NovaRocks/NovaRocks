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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, new_empty_array};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::expr::{ExprArena, cast_array_to_target};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::lower::expr::lower_t_expr;
use crate::lower::layout::{Layout, layout_from_slot_ids};
use crate::lower::node::Lowered;
use crate::{plan_nodes, types};

/// Lower a UNION_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_union_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    mut out_layout: Layout,
    arena: &mut ExprArena,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let Some(un) = node.union_node.as_ref() else {
        return Err("UNION_NODE missing union_node payload".to_string());
    };
    if out_layout.order.is_empty() {
        let tuple_id = un.tuple_id;
        let col_count = un
            .const_expr_lists
            .get(0)
            .map(|r| r.len())
            .or_else(|| un.result_expr_lists.get(0).map(|r| r.len()))
            .unwrap_or(0);
        out_layout = layout_from_slot_ids(tuple_id, (0..col_count).map(|i| i as types::TSlotId));
    }

    let output_slots = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<Vec<_>, _>>()?;

    let mut inputs = Vec::new();

    if !un.const_expr_lists.is_empty() {
        let row_count = un.const_expr_lists.len();
        let col_count = un.const_expr_lists[0].len();
        let mut columns = vec![Vec::with_capacity(row_count); col_count];
        let dummy_chunk = empty_chunk_with_rows(1)?;
        for row_exprs in &un.const_expr_lists {
            if row_exprs.len() != col_count {
                return Err("UNION_NODE const_expr_lists column size mismatch".to_string());
            }
            for (col_idx, e) in row_exprs.iter().enumerate() {
                let expr_id = lower_t_expr(e, arena, &out_layout, last_query_id, fe_addr)?;
                let array = arena.eval(expr_id, &dummy_chunk)?;
                let expr_type = arena
                    .data_type(expr_id)
                    .cloned()
                    .ok_or_else(|| "UNION const expr missing inferred data type".to_string())?;
                let normalized = normalize_const_array(&array, &expr_type)?;
                columns[col_idx].push(normalized);
            }
        }
        let slot_ids = output_slots.clone();
        let chunk = chunk_from_const_arrays(columns, row_count, &slot_ids)?;
        inputs.push(ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk,
                node_id: node.node_id,
            }),
        });
    }

    if !children.is_empty() {
        if un.result_expr_lists.is_empty() {
            return Err(
                "UNION_NODE requires result_expr_lists for materialized children".to_string(),
            );
        }
        if un.result_expr_lists.len() != children.len() {
            return Err(format!(
                "UNION_NODE result_expr_lists size mismatch: expr_lists={} children={}",
                un.result_expr_lists.len(),
                children.len()
            ));
        }
        for (child, expr_list) in children.into_iter().zip(un.result_expr_lists.iter()) {
            let mut exprs = Vec::with_capacity(expr_list.len());
            for e in expr_list {
                exprs.push(lower_t_expr(
                    e,
                    arena,
                    &child.layout,
                    last_query_id,
                    fe_addr,
                )?);
            }
            inputs.push(ExecNode {
                kind: ExecNodeKind::Project(ProjectNode {
                    input: Box::new(child.node),
                    node_id: node.node_id,
                    is_subordinate: true,
                    exprs,
                    expr_slot_ids: output_slots.clone(),
                    output_indices: None,
                    output_slots: output_slots.clone(),
                }),
            });
        }
    }

    if inputs.len() == 1 {
        Ok(Lowered {
            node: inputs.into_iter().next().expect("one input"),
            layout: out_layout,
        })
    } else {
        Ok(Lowered {
            node: ExecNode {
                kind: ExecNodeKind::UnionAll(UnionAllNode {
                    inputs,
                    node_id: node.node_id,
                }),
            },
            layout: out_layout,
        })
    }
}

// NOTE: literal parsing helpers for UNION const values

fn empty_chunk_with_rows(row_count: usize) -> Result<Chunk, String> {
    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let batch =
        RecordBatch::try_new_with_options(schema, vec![], &options).map_err(|e| e.to_string())?;
    Ok(Chunk::new(batch))
}

fn normalize_const_array(array: &ArrayRef, expected_type: &DataType) -> Result<ArrayRef, String> {
    if array.len() != 1 {
        return Err(format!(
            "const expr should return single-row array, got {} rows",
            array.len()
        ));
    }
    if matches!(expected_type, DataType::Null) || array.data_type() == expected_type {
        return Ok(Arc::clone(array));
    }
    cast_array_to_target(array, expected_type).map_err(|e| {
        format!(
            "const expr cast failed from {:?} to {:?}: {}",
            array.data_type(),
            expected_type,
            e
        )
    })
}

fn concat_const_column(arrays: &[ArrayRef]) -> Result<ArrayRef, String> {
    let target = infer_const_column_target(arrays)?;
    if arrays.is_empty() {
        return Ok(new_empty_array(&target));
    }
    let mut casted = Vec::with_capacity(arrays.len());
    for array in arrays {
        if array.data_type() == &target {
            casted.push(Arc::clone(array));
        } else {
            let casted_array = cast_array_to_target(array, &target)?;
            casted.push(casted_array);
        }
    }
    if casted.len() == 1 {
        return Ok(casted.into_iter().next().expect("one array"));
    }
    let refs: Vec<&dyn Array> = casted.iter().map(|array| array.as_ref()).collect();
    concat(&refs).map_err(|e| e.to_string())
}

fn infer_const_column_target(arrays: &[ArrayRef]) -> Result<DataType, String> {
    let mut target = DataType::Null;
    for array in arrays {
        target = merge_const_column_type(&target, array.data_type())?;
    }
    Ok(target)
}

fn merge_const_column_type(current: &DataType, candidate: &DataType) -> Result<DataType, String> {
    if current == candidate {
        return Ok(current.clone());
    }
    if matches!(current, DataType::Null) {
        return Ok(candidate.clone());
    }
    if matches!(candidate, DataType::Null) {
        return Ok(current.clone());
    }

    match (current, candidate) {
        (DataType::Int8, DataType::Int16)
        | (DataType::Int8, DataType::Int32)
        | (DataType::Int8, DataType::Int64)
        | (DataType::Int16, DataType::Int8)
        | (DataType::Int16, DataType::Int32)
        | (DataType::Int16, DataType::Int64)
        | (DataType::Int32, DataType::Int8)
        | (DataType::Int32, DataType::Int16)
        | (DataType::Int32, DataType::Int64)
        | (DataType::Int64, DataType::Int8)
        | (DataType::Int64, DataType::Int16)
        | (DataType::Int64, DataType::Int32) => Ok(DataType::Int64),
        (DataType::Float32, DataType::Float64) | (DataType::Float64, DataType::Float32) => {
            Ok(DataType::Float64)
        }
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            let scale = (*s1).max(*s2);
            let int_digits_1 = i16::from(*p1) - i16::from(*s1);
            let int_digits_2 = i16::from(*p2) - i16::from(*s2);
            let int_digits = int_digits_1.max(int_digits_2);
            let precision_i16 = int_digits + i16::from(scale);
            let precision = u8::try_from(precision_i16.max(1))
                .map_err(|_| "const decimal precision overflow".to_string())?;
            if precision > 38 {
                return Err(format!(
                    "const decimal precision overflow: current={:?} candidate={:?}",
                    current, candidate
                ));
            }
            Ok(DataType::Decimal128(precision, scale))
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => {
            let scale = (*s1).max(*s2);
            let int_digits_1 = i16::from(*p1) - i16::from(*s1);
            let int_digits_2 = i16::from(*p2) - i16::from(*s2);
            let int_digits = int_digits_1.max(int_digits_2);
            let precision_i16 = int_digits + i16::from(scale);
            let precision = u8::try_from(precision_i16.max(1))
                .map_err(|_| "const decimal precision overflow".to_string())?;
            if precision > 76 {
                return Err(format!(
                    "const decimal precision overflow: current={:?} candidate={:?}",
                    current, candidate
                ));
            }
            Ok(DataType::Decimal256(precision, scale))
        }
        (DataType::Decimal128(p1, s1), DataType::Decimal256(p2, s2))
        | (DataType::Decimal256(p2, s2), DataType::Decimal128(p1, s1)) => {
            let scale = (*s1).max(*s2);
            let int_digits_1 = i16::from(*p1) - i16::from(*s1);
            let int_digits_2 = i16::from(*p2) - i16::from(*s2);
            let int_digits = int_digits_1.max(int_digits_2);
            let precision_i16 = int_digits + i16::from(scale);
            let precision = u8::try_from(precision_i16.max(1))
                .map_err(|_| "const decimal precision overflow".to_string())?;
            if precision > 76 {
                return Err(format!(
                    "const decimal precision overflow: current={:?} candidate={:?}",
                    current, candidate
                ));
            }
            Ok(DataType::Decimal256(precision, scale))
        }
        (DataType::List(item1), DataType::List(item2)) => {
            let merged_item = merge_const_field(item1, item2)?;
            Ok(DataType::List(Arc::new(merged_item)))
        }
        (DataType::Struct(fields1), DataType::Struct(fields2)) => {
            if fields1.len() != fields2.len() {
                return Err(format!(
                    "const struct field count mismatch: {} vs {}",
                    fields1.len(),
                    fields2.len()
                ));
            }
            let mut merged_fields = Vec::with_capacity(fields1.len());
            for (lhs, rhs) in fields1.iter().zip(fields2.iter()) {
                if lhs.name() != rhs.name() {
                    return Err(format!(
                        "const struct field name mismatch: {} vs {}",
                        lhs.name(),
                        rhs.name()
                    ));
                }
                merged_fields.push(merge_const_field(lhs, rhs)?);
            }
            Ok(DataType::Struct(merged_fields.into()))
        }
        (DataType::Map(entries1, ordered1), DataType::Map(entries2, ordered2)) => {
            if ordered1 != ordered2 {
                return Err(format!(
                    "const MAP ordered flag mismatch: {} vs {}",
                    ordered1, ordered2
                ));
            }
            let merged_entries = merge_const_field(entries1, entries2)?;
            Ok(DataType::Map(Arc::new(merged_entries), *ordered1))
        }
        _ => Err(format!(
            "const values column type mismatch: {:?} vs {:?}",
            current, candidate
        )),
    }
}

fn merge_const_field(current: &Field, candidate: &Field) -> Result<Field, String> {
    let merged_type = merge_const_column_type(current.data_type(), candidate.data_type())?;
    Ok(current
        .clone()
        .with_data_type(merged_type)
        .with_nullable(current.is_nullable() || candidate.is_nullable()))
}

fn chunk_from_const_arrays(
    columns: Vec<Vec<ArrayRef>>,
    row_count: usize,
    slot_ids: &[SlotId],
) -> Result<Chunk, String> {
    let num_rows = if columns.is_empty() {
        row_count
    } else {
        columns[0].len()
    };
    if num_rows != row_count {
        return Err("const values row count mismatch".to_string());
    }
    for (idx, col) in columns.iter().enumerate() {
        if col.len() != num_rows {
            return Err(format!("const values column length mismatch: {idx}"));
        }
    }
    if slot_ids.len() != columns.len() {
        return Err(format!(
            "const values slot count mismatch: slots={} columns={}",
            slot_ids.len(),
            columns.len()
        ));
    }

    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        let array = concat_const_column(col)?;
        let field = field_with_slot_id(
            Field::new(format!("col_{idx}"), array.data_type().clone(), true),
            slot_ids[idx],
        );
        fields.push(field);
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = if arrays.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(schema, arrays, &options)
    } else {
        RecordBatch::try_new(schema, arrays)
    }
    .map_err(|e| e.to_string())?;
    Ok(Chunk::new(batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exprs;
    use arrow::array::{StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::TimeUnit;
    use std::collections::HashMap;

    #[test]
    fn normalize_const_array_casts_utf8_to_timestamp() {
        let input = Arc::new(StringArray::from(vec!["2024-02-29 23:59:59"])) as ArrayRef;
        let expected = DataType::Timestamp(TimeUnit::Microsecond, None);
        let out = normalize_const_array(&input, &expected).expect("cast should succeed");
        assert_eq!(out.data_type(), &expected);
        let ts = out
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("must be timestamp array");
        assert_eq!(ts.len(), 1);
        assert!(!ts.is_null(0));
    }

    #[test]
    fn normalize_const_array_rejects_multi_row_const() {
        let input = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let err = normalize_const_array(&input, &DataType::Utf8).expect_err("must fail");
        assert!(err.contains("single-row"));
    }

    #[test]
    fn merge_const_column_type_merges_decimal256_precision() {
        let merged =
            merge_const_column_type(&DataType::Decimal256(76, 10), &DataType::Decimal256(74, 10))
                .expect("merge decimal256");
        assert_eq!(merged, DataType::Decimal256(76, 10));
    }

    #[test]
    fn merge_const_column_type_promotes_decimal128_with_decimal256() {
        let merged =
            merge_const_column_type(&DataType::Decimal128(38, 4), &DataType::Decimal256(70, 10))
                .expect("promote to decimal256");
        assert_eq!(merged, DataType::Decimal256(70, 10));
    }

    fn single_slot_layout(tuple_id: i32, slot_id: i32) -> Layout {
        let mut index = HashMap::new();
        index.insert((tuple_id, slot_id), 0);
        Layout {
            order: vec![(tuple_id, slot_id)],
            index,
        }
    }

    fn dummy_child(layout: Layout) -> Lowered {
        Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id: 0,
                }),
            },
            layout,
        }
    }

    fn union_plan_node(result_expr_lists: Vec<Vec<exprs::TExpr>>) -> plan_nodes::TPlanNode {
        plan_nodes::TPlanNode {
            node_id: 9,
            node_type: plan_nodes::TPlanNodeType::UNION_NODE,
            num_children: 1,
            limit: -1,
            row_tuples: vec![],
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
            union_node: Some(plan_nodes::TUnionNode {
                tuple_id: 1,
                result_expr_lists,
                const_expr_lists: vec![],
                first_materialized_child_idx: 0,
                pass_through_slot_maps: None,
                local_exchanger_type: None,
                local_partition_by_exprs: None,
            }),
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
        }
    }

    #[test]
    fn lower_union_node_rejects_missing_result_expr_lists_for_children() {
        let children = vec![dummy_child(single_slot_layout(1, 1))];
        let node = union_plan_node(vec![]);
        let mut arena = ExprArena::default();
        let out_layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };

        let err = lower_union_node(children, &node, out_layout, &mut arena, None, None)
            .expect_err("missing result_expr_lists must fail");
        assert!(err.contains("requires result_expr_lists"));
    }

    #[test]
    fn lower_union_node_rejects_result_expr_lists_child_count_mismatch() {
        let children = vec![dummy_child(single_slot_layout(1, 1))];
        let node = union_plan_node(vec![vec![], vec![]]);
        let mut arena = ExprArena::default();
        let out_layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };

        let err = lower_union_node(children, &node, out_layout, &mut arena, None, None)
            .expect_err("result_expr_lists child-count mismatch must fail");
        assert!(err.contains("result_expr_lists size mismatch"));
    }
}
