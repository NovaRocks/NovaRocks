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

use crate::common::ids::SlotId;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::{layout_for_row_tuples, layout_for_tuple_columns, resolve_mysql_table};
use crate::lower::node::{Lowered, local_rf_waiting_set};
use crate::novarocks_connectors::{ConnectorRegistry, JdbcScanConfig, ScanConfig};
use crate::{descriptors, internal_service, plan_nodes, types};

/// Lower a MYSQL_SCAN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_mysql_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    query_opts: Option<&internal_service::TQueryOptions>,
    connectors: &ConnectorRegistry,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "MYSQL_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }

    let Some(desc_tbl) = desc_tbl else {
        return Err("MYSQL_SCAN_NODE requires desc_tbl".to_string());
    };
    let Some(mysql) = node.mysql_scan_node.as_ref() else {
        return Err("MYSQL_SCAN_NODE missing mysql_scan_node payload".to_string());
    };

    let tuple_id = mysql.tuple_id;

    // Ensure slot-ref indices match scan output order.
    let out_layout = match layout_for_tuple_columns(desc_tbl, tuple_id, &mysql.columns) {
        Ok(layout) => layout,
        Err(_) => layout_for_row_tuples(&[tuple_id], tuple_slots),
    };
    if out_layout.order.is_empty() {
        return Err(format!(
            "MYSQL_SCAN_NODE tuple_id={tuple_id} has empty output layout"
        ));
    }

    let tbl = resolve_mysql_table(desc_tbl, tuple_id)?;
    let slot_ids = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<Vec<_>, _>>()?;
    let output_slots = slot_ids.clone();
    let limit = mysql.limit.and_then(|v| (v >= 0).then_some(v as usize));
    let cfg = JdbcScanConfig {
        jdbc_url: format!("mysql://{}:{}/{}", tbl.host, tbl.port, tbl.db),
        jdbc_user: Some(tbl.user.clone()),
        jdbc_passwd: Some(tbl.passwd.clone()),
        table: format!("{}.{}", tbl.db, tbl.table),
        columns: mysql.columns.clone(),
        filters: mysql.filters.clone(),
        limit,
        slot_ids,
    };
    let connector_io_tasks_per_scan_operator =
        query_opts.and_then(|opts| opts.connector_io_tasks_per_scan_operator);
    let scan = connectors
        .create_scan_node("mysql", ScanConfig::Jdbc(cfg))?
        .with_node_id(node.node_id)
        .with_output_slots(output_slots)
        .with_limit(limit)
        .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
        .with_local_rf_waiting_set(local_rf_waiting_set(node));
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: out_layout,
    })
}
