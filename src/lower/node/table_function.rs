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
use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::common::ids::SlotId;
use crate::common::largeint;
use crate::exec::node::table_function::{TableFunctionNode, TableFunctionOutputSlot};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::{Layout, schema_for_layout};
use crate::lower::node::Lowered;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::{descriptors, plan_nodes};

pub(crate) fn lower_table_function_node(
    child: Lowered,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
) -> Result<Lowered, String> {
    let is_integer_like = |dt: &DataType| {
        matches!(
            dt,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ) || largeint::is_largeint_data_type(dt)
    };

    let Some(table_fn_node) = node.table_function_node.as_ref() else {
        return Err("TABLE_FUNCTION_NODE missing table_function_node payload".to_string());
    };
    let param_columns = table_fn_node
        .param_columns
        .as_ref()
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing param_columns".to_string())?;
    let outer_columns = table_fn_node
        .outer_columns
        .as_ref()
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing outer_columns".to_string())?;
    let fn_result_columns = table_fn_node
        .fn_result_columns
        .as_ref()
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing fn_result_columns".to_string())?;
    let fn_result_required = table_fn_node
        .fn_result_required
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing fn_result_required".to_string())?;
    let table_fn_expr = table_fn_node
        .table_function
        .as_ref()
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing table_function".to_string())?;
    if table_fn_expr.nodes.len() != 1 {
        return Err(format!(
            "TABLE_FUNCTION_NODE expects single expr node, got {}",
            table_fn_expr.nodes.len()
        ));
    }
    let expr_node = table_fn_expr
        .nodes
        .first()
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing table function expr node".to_string())?;
    let func = expr_node
        .fn_
        .as_ref()
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing function in expr node".to_string())?;
    let func_name = func.name.function_name.clone();
    let table_fn = func
        .table_fn
        .as_ref()
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing table_fn in function".to_string())?;

    if func.arg_types.len() != param_columns.len() {
        return Err(format!(
            "TABLE_FUNCTION_NODE param_columns size mismatch: expected {} got {}",
            func.arg_types.len(),
            param_columns.len()
        ));
    }

    let mut param_types = Vec::with_capacity(func.arg_types.len());
    for (idx, t) in func.arg_types.iter().enumerate() {
        let dt = arrow_type_from_desc(t)
            .ok_or_else(|| format!("TABLE_FUNCTION_NODE param type {idx} is unsupported"))?;
        param_types.push(dt);
    }

    let mut ret_types = Vec::with_capacity(table_fn.ret_types.len());
    for (idx, t) in table_fn.ret_types.iter().enumerate() {
        let dt = arrow_type_from_desc(t)
            .ok_or_else(|| format!("TABLE_FUNCTION_NODE return type {idx} is unsupported"))?;
        ret_types.push(dt);
    }

    let function_name = func_name.to_ascii_lowercase();
    match function_name.as_str() {
        "unnest" => {
            if param_types.is_empty() {
                return Err(
                    "TABLE_FUNCTION_NODE unnest requires at least one parameter".to_string()
                );
            }
            if param_types.len() != ret_types.len() {
                return Err(format!(
                    "TABLE_FUNCTION_NODE unnest return type count mismatch: params={} returns={}",
                    param_types.len(),
                    ret_types.len()
                ));
            }
            for (idx, (param, ret)) in param_types.iter().zip(ret_types.iter()).enumerate() {
                let DataType::List(item) = param else {
                    return Err(format!(
                        "TABLE_FUNCTION_NODE unnest param {idx} must be ARRAY, got {:?}",
                        param
                    ));
                };
                if item.data_type() != ret {
                    return Err(format!(
                        "TABLE_FUNCTION_NODE unnest param {idx} item type mismatch: expected {:?} got {:?}",
                        ret,
                        item.data_type()
                    ));
                }
            }
        }
        "unnest_bitmap" => {
            if param_types.len() != 1 {
                return Err(format!(
                    "TABLE_FUNCTION_NODE unnest_bitmap expects 1 arg, got {}",
                    param_types.len()
                ));
            }
            if !matches!(param_types[0], DataType::Binary) {
                return Err(format!(
                    "TABLE_FUNCTION_NODE unnest_bitmap param must be BITMAP/BINARY, got {:?}",
                    param_types[0]
                ));
            }
            if ret_types.len() != 1 {
                return Err(format!(
                    "TABLE_FUNCTION_NODE unnest_bitmap must have exactly 1 return column, got {}",
                    ret_types.len()
                ));
            }
            if !matches!(ret_types[0], DataType::Int64) {
                return Err(format!(
                    "TABLE_FUNCTION_NODE unnest_bitmap return type must be BIGINT, got {:?}",
                    ret_types[0]
                ));
            }
        }
        "subdivide_bitmap" => {
            if param_types.len() != 2 {
                return Err(format!(
                    "TABLE_FUNCTION_NODE subdivide_bitmap expects 2 args, got {}",
                    param_types.len()
                ));
            }
            if !matches!(param_types[0], DataType::Binary) {
                return Err(format!(
                    "TABLE_FUNCTION_NODE subdivide_bitmap param 0 must be BITMAP/BINARY, got {:?}",
                    param_types[0]
                ));
            }
            if !is_integer_like(&param_types[1]) {
                return Err(format!(
                    "TABLE_FUNCTION_NODE subdivide_bitmap param 1 must be integer, got {:?}",
                    param_types[1]
                ));
            }
            if ret_types.len() != 1 {
                return Err(format!(
                    "TABLE_FUNCTION_NODE subdivide_bitmap must have exactly 1 return column, got {}",
                    ret_types.len()
                ));
            }
            if !matches!(ret_types[0], DataType::Binary) {
                return Err(format!(
                    "TABLE_FUNCTION_NODE subdivide_bitmap return type must be BITMAP/BINARY, got {:?}",
                    ret_types[0]
                ));
            }
        }
        "generate_series" => {
            if !(param_types.len() == 2 || param_types.len() == 3) {
                return Err(format!(
                    "TABLE_FUNCTION_NODE generate_series expects 2 or 3 args, got {}",
                    param_types.len()
                ));
            }
            for (idx, param) in param_types.iter().enumerate() {
                if !is_integer_like(param) {
                    return Err(format!(
                        "TABLE_FUNCTION_NODE generate_series param {idx} must be TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {:?}",
                        param
                    ));
                }
            }
            if ret_types.len() != 1 {
                return Err(format!(
                    "TABLE_FUNCTION_NODE generate_series must have exactly 1 return column, got {}",
                    ret_types.len()
                ));
            }
            if !is_integer_like(&ret_types[0]) {
                return Err(format!(
                    "TABLE_FUNCTION_NODE generate_series return type must be TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {:?}",
                    ret_types[0]
                ));
            }
        }
        _ => {
            return Err(format!(
                "TABLE_FUNCTION_NODE unsupported table function: {}",
                func_name
            ));
        }
    }

    let param_slots: Vec<SlotId> = param_columns
        .iter()
        .map(|sid| SlotId::try_from(*sid))
        .collect::<Result<Vec<_>, _>>()?;
    let outer_slots: Vec<SlotId> = outer_columns
        .iter()
        .map(|sid| SlotId::try_from(*sid))
        .collect::<Result<Vec<_>, _>>()?;
    let fn_result_slots: Vec<SlotId> = fn_result_columns
        .iter()
        .map(|sid| SlotId::try_from(*sid))
        .collect::<Result<Vec<_>, _>>()?;
    if fn_result_slots.len() != ret_types.len() {
        return Err(format!(
            "TABLE_FUNCTION_NODE fn_result_columns size mismatch: slots={} returns={}",
            fn_result_slots.len(),
            ret_types.len()
        ));
    }

    let child_slots: HashSet<SlotId> = child
        .layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<HashSet<_>, _>>()?;
    for slot in param_slots.iter().chain(outer_slots.iter()) {
        if !child_slots.contains(slot) {
            return Err(format!(
                "TABLE_FUNCTION_NODE input slot {} not found in child layout",
                slot
            ));
        }
    }

    let schema = {
        let desc_tbl =
            desc_tbl.ok_or_else(|| "TABLE_FUNCTION_NODE missing descriptor table".to_string())?;
        schema_for_layout(desc_tbl, &out_layout)?
    };
    let mut output_slots = Vec::with_capacity(out_layout.order.len());
    for (_, slot_id) in out_layout.order.iter() {
        output_slots.push(SlotId::try_from(*slot_id)?);
    }

    if schema.fields().len() != output_slots.len() {
        return Err(format!(
            "TABLE_FUNCTION_NODE output schema size mismatch: schema={} slots={}",
            schema.fields().len(),
            output_slots.len()
        ));
    }

    let mut outer_map = HashMap::new();
    for slot in &outer_slots {
        outer_map.insert(*slot, ());
    }
    let mut result_map = HashMap::new();
    for (idx, slot) in fn_result_slots.iter().enumerate() {
        result_map.insert(*slot, idx);
    }

    let mut output_slot_sources = Vec::with_capacity(output_slots.len());
    for slot in &output_slots {
        if outer_map.contains_key(slot) {
            output_slot_sources.push(TableFunctionOutputSlot::Outer { slot: *slot });
            continue;
        }
        if let Some(idx) = result_map.get(slot).copied() {
            if !fn_result_required {
                return Err(format!(
                    "TABLE_FUNCTION_NODE output includes result slot {} but fn_result_required=false",
                    slot
                ));
            }
            output_slot_sources.push(TableFunctionOutputSlot::Result { index: idx });
            continue;
        }
        return Err(format!(
            "TABLE_FUNCTION_NODE output slot {} is neither outer nor result",
            slot
        ));
    }

    let is_left_join = table_fn
        .is_left_join
        .ok_or_else(|| "TABLE_FUNCTION_NODE missing is_left_join".to_string())?;

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::TableFunction(TableFunctionNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                function_name: func_name,
                param_slots,
                outer_slots,
                fn_result_slots,
                fn_result_required,
                is_left_join,
                param_types,
                ret_types,
                output_schema: schema,
                output_slots,
                output_slot_sources,
            }),
        },
        layout: out_layout,
    })
}
