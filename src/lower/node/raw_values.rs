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

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::lower::layout::{Layout, layout_from_slot_ids};
use crate::lower::node::Lowered;

use crate::plan_nodes;

/// Lower a RAW_VALUES_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_raw_values_node(
    node: &plan_nodes::TPlanNode,
    out_layout: &mut Layout,
) -> Result<Lowered, String> {
    let Some(raw) = node.raw_values_node.as_ref() else {
        return Err("RAW_VALUES_NODE missing raw_values_node payload".to_string());
    };

    if out_layout.order.is_empty() {
        *out_layout = layout_from_slot_ids(raw.tuple_id, [0].into_iter());
    }

    if let Some(vals) = raw.long_values.as_ref() {
        let array = Int64Array::from_iter(vals.iter().copied().map(Some));
        let slot_id = out_layout
            .order
            .get(0)
            .map(|(_, slot)| *slot)
            .ok_or_else(|| "RAW_VALUES_NODE missing output slot id".to_string())?;
        let slot_id = SlotId::try_from(slot_id)?;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("col_0", DataType::Int64, true),
            slot_id,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(array) as _]).map_err(|e| e.to_string())?;
        let chunk = Chunk::new(batch);
        return Ok(Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk,
                    node_id: node.node_id,
                }),
            },
            layout: out_layout.clone(),
        });
    } else if let Some(vals) = raw.string_values.as_ref() {
        let array = StringArray::from_iter(vals.iter().map(|v| Some(v.as_str())));
        let slot_id = out_layout
            .order
            .get(0)
            .map(|(_, slot)| *slot)
            .ok_or_else(|| "RAW_VALUES_NODE missing output slot id".to_string())?;
        let slot_id = SlotId::try_from(slot_id)?;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("col_0", DataType::Utf8, true),
            slot_id,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(array) as _]).map_err(|e| e.to_string())?;
        let chunk = Chunk::new(batch);
        return Ok(Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk,
                    node_id: node.node_id,
                }),
            },
            layout: out_layout.clone(),
        });
    }

    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let chunk = Chunk::new(batch);
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk,
                node_id: node.node_id,
            }),
        },
        layout: out_layout.clone(),
    })
}
