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
use crate::lower::layout::{
    jdbc_conn_from_config, layout_for_row_tuples, layout_from_slot_ids, qualify_table_name,
    resolve_jdbc_table, resolve_jdbc_table_by_name, tuple_slot_col_names,
};
use crate::lower::node::{Lowered, local_rf_waiting_set};
use crate::novarocks_connectors::{ConnectorRegistry, JdbcScanConfig, ScanConfig};
use crate::{descriptors, internal_service, plan_nodes, types};

/// Lower a JDBC_SCAN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_jdbc_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    query_opts: Option<&internal_service::TQueryOptions>,
    connectors: &ConnectorRegistry,
    db_name: Option<&str>,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "JDBC_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }

    let Some(jdbc) = node.jdbc_scan_node.as_ref() else {
        return Err("JDBC_SCAN_NODE missing jdbc_scan_node payload".to_string());
    };
    let tuple_id = jdbc
        .tuple_id
        .or_else(|| node.row_tuples.get(0).copied())
        .ok_or_else(|| "JDBC_SCAN_NODE missing tuple_id".to_string())?;

    let mut out_layout = crate::lower::layout::Layout {
        order: Vec::new(),
        index: HashMap::new(),
    };
    if out_layout.order.is_empty() {
        if let Some(hint) = layout_hints.get(&tuple_id).filter(|v| !v.is_empty()) {
            out_layout = layout_from_slot_ids(tuple_id, hint.iter().copied());
        } else if let Some(cols) = jdbc.columns.as_ref().filter(|c| !c.is_empty()) {
            out_layout =
                layout_from_slot_ids(tuple_id, (0..cols.len()).map(|i| i as types::TSlotId));
        } else {
            out_layout = layout_for_row_tuples(&[tuple_id], tuple_slots);
        }
    }
    if out_layout.order.is_empty() {
        return Err(format!(
            "JDBC_SCAN_NODE tuple_id={tuple_id} has empty output layout"
        ));
    }

    let (jdbc_url, jdbc_user, jdbc_passwd, jdbc_table_name) = if let Some(desc_tbl) = desc_tbl {
        match resolve_jdbc_table(desc_tbl, tuple_id)
            .or_else(|_| resolve_jdbc_table_by_name(desc_tbl, jdbc.table_name.as_deref()))
        {
            Ok(jdbc_table) => {
                let jdbc_url = jdbc_table
                    .jdbc_url
                    .as_ref()
                    .ok_or_else(|| "JDBC table missing jdbc_url".to_string())?
                    .clone();
                (
                    jdbc_url,
                    jdbc_table.jdbc_user.clone(),
                    jdbc_table.jdbc_passwd.clone(),
                    jdbc_table.jdbc_table.clone(),
                )
            }
            Err(_) => {
                let (jdbc_url, user, pass) = jdbc_conn_from_config()?;
                (jdbc_url, user, pass, None)
            }
        }
    } else {
        let (jdbc_url, user, pass) = jdbc_conn_from_config()?;
        (jdbc_url, user, pass, None)
    };

    let table = jdbc
        .table_name
        .clone()
        .or(jdbc_table_name)
        .ok_or_else(|| "JDBC_SCAN_NODE missing table name".to_string())?;
    let table = qualify_table_name(table, db_name);

    let columns = match jdbc.columns.as_ref().filter(|c| !c.is_empty()) {
        Some(cols) => cols.clone(),
        None => desc_tbl
            .and_then(|d| tuple_slot_col_names(d, tuple_id, tuple_slots).ok())
            .unwrap_or_default(),
    };
    let filters = jdbc.filters.clone().unwrap_or_default();
    let limit = jdbc.limit.and_then(|v| (v >= 0).then_some(v as usize));
    let connector_io_tasks_per_scan_operator =
        query_opts.and_then(|opts| opts.connector_io_tasks_per_scan_operator);

    let slot_ids = out_layout
        .order
        .iter()
        .map(|(_, slot)| SlotId::try_from(*slot))
        .collect::<Result<Vec<_>, _>>()?;
    let output_slots = slot_ids.clone();
    let cfg = JdbcScanConfig {
        jdbc_url,
        jdbc_user,
        jdbc_passwd,
        table,
        columns,
        filters,
        limit,
        slot_ids,
    };

    let scan = connectors
        .create_scan_node("jdbc", ScanConfig::Jdbc(cfg))?
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
