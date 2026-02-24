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

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::descriptors;
use crate::exec::chunk::Chunk;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::{Layout, schema_for_layout};
use crate::lower::node::Lowered;
use crate::plan_nodes;
use crate::novarocks_logging::warn;

/// Lower a SCHEMA_SCAN_NODE to an empty `ValuesNode`.
///
/// This unblocks FE internal maintenance jobs that reference information_schema
/// while we incrementally align full schema-scan semantics.
pub(crate) fn lower_schema_scan_node(
    node: &plan_nodes::TPlanNode,
    out_layout: &Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
) -> Result<Lowered, String> {
    let schema_scan = node
        .schema_scan_node
        .as_ref()
        .ok_or_else(|| "SCHEMA_SCAN_NODE missing schema_scan_node payload".to_string())?;

    let schema = if out_layout.order.is_empty() {
        Arc::new(Schema::empty())
    } else {
        let desc_tbl =
            desc_tbl.ok_or_else(|| "SCHEMA_SCAN_NODE requires desc_tbl for schema".to_string())?;
        schema_for_layout(desc_tbl, out_layout)?
    };
    warn!(
        "SCHEMA_SCAN_NODE is lowered to empty values for table_name={} db={:?}",
        schema_scan.table_name, schema_scan.db
    );
    let chunk = Chunk::new(RecordBatch::new_empty(schema));
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
