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
use crate::lower::expr::parse_min_max_conjunct;
use crate::lower::layout::{
    Layout, layout_for_row_tuples, layout_from_slot_ids, schema_for_layout, schema_for_tuple,
};
use crate::lower::node::{Lowered, local_rf_waiting_set};
use crate::novarocks_connectors::{
    ConnectorRegistry, ScanConfig, StarRocksScanConfig, StarRocksScanRange,
};
use crate::novarocks_logging::debug;
use crate::{descriptors, internal_service, plan_nodes, types};

/// Lower a STARROCKS_SCAN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_starrocks_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    connectors: &ConnectorRegistry,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "STARROCKS_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }

    let Some(sr) = node.starrocks_scan_node.as_ref() else {
        return Err("STARROCKS_SCAN_NODE missing starrocks_scan_node payload".to_string());
    };
    let tuple_id = sr
        .tuple_id
        .or_else(|| node.row_tuples.get(0).copied())
        .ok_or_else(|| "STARROCKS_SCAN_NODE missing tuple_id".to_string())?;

    let mut out_layout = Layout {
        order: Vec::new(),
        index: HashMap::new(),
    };
    if out_layout.order.is_empty() {
        if let Some(hint) = layout_hints.get(&tuple_id).filter(|v| !v.is_empty()) {
            out_layout = layout_from_slot_ids(tuple_id, hint.iter().copied());
        } else {
            out_layout = layout_for_row_tuples(&[tuple_id], tuple_slots);
        }
    }
    if out_layout.order.is_empty() {
        return Err(format!(
            "STARROCKS_SCAN_NODE tuple_id={tuple_id} has empty output layout"
        ));
    }

    let desc_tbl = desc_tbl.ok_or_else(|| {
        format!(
            "STARROCKS_SCAN_NODE node_id={} requires descriptor table for schema",
            node.node_id
        )
    })?;
    let schema = schema_for_layout(desc_tbl, &out_layout)?;
    let required_schema = schema_for_tuple(desc_tbl, tuple_id)?;

    let slot_ids = out_layout
        .order
        .iter()
        .map(|(_, s)| SlotId::try_from(*s))
        .collect::<Result<Vec<_>, _>>()?;
    if !slot_ids.is_empty() && slot_ids.len() != schema.fields().len() {
        return Err(format!(
            "STARROCKS_SCAN_NODE output layout/schema mismatch: layout_len={}, schema_len={}",
            slot_ids.len(),
            schema.fields().len()
        ));
    }

    let Some(exec_params) = exec_params else {
        return Err("STARROCKS_SCAN_NODE requires exec_params.per_node_scan_ranges".to_string());
    };
    let scan_ranges = exec_params
        .per_node_scan_ranges
        .get(&node.node_id)
        .ok_or_else(|| format!("missing per_node_scan_ranges for node_id={}", node.node_id))?;

    let mut ranges = Vec::new();
    let mut has_more = false;
    for p in scan_ranges {
        if p.empty.unwrap_or(false) {
            if p.has_more.unwrap_or(false) {
                has_more = true;
            }
            continue;
        }
        let Some(internal) = p.scan_range.internal_scan_range.as_ref() else {
            continue;
        };
        let version = internal.version.parse::<i64>().ok().filter(|v| *v >= 0);
        let schema_hash = internal.schema_hash.parse::<i32>().ok().filter(|v| *v > 0);
        let fill_data_cache = internal.fill_data_cache.unwrap_or(true);
        let skip_page_cache = internal.skip_page_cache.unwrap_or(false);
        let skip_disk_cache = internal.skip_disk_cache.unwrap_or(false);
        if !fill_data_cache || skip_page_cache || skip_disk_cache {
            return Err(format!(
                "STARROCKS_SCAN_NODE node_id={} does not support internal-table cache controls yet (fill_data_cache={}, skip_page_cache={}, skip_disk_cache={})",
                node.node_id, fill_data_cache, skip_page_cache, skip_disk_cache
            ));
        }

        ranges.push(StarRocksScanRange {
            tablet_id: internal.tablet_id,
            version,
            schema_hash,
            db_name: Some(internal.db_name.clone()),
            table_name: internal.table_name.clone().filter(|s| !s.is_empty()),
            hosts: internal.hosts.clone(),
            fill_data_cache,
            skip_page_cache,
            skip_disk_cache,
        });
    }

    if has_more {
        return Err(format!(
            "STARROCKS_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node.node_id
        ));
    }

    debug!(
        "STARROCKS_SCAN_NODE node_id={} ranges={}, tuple_id={}",
        node.node_id,
        ranges.len(),
        tuple_id
    );

    let limit = (node.limit >= 0).then_some(node.limit as usize);
    let batch_size = query_opts.and_then(|opts| opts.batch_size).or(Some(4096));
    let query_timeout = query_opts.and_then(|opts| opts.query_timeout);
    let mem_limit = query_opts
        .and_then(|opts| opts.query_mem_limit.or(opts.mem_limit))
        .filter(|v| *v > 0);
    let connector_io_tasks_per_scan_operator =
        query_opts.and_then(|opts| opts.connector_io_tasks_per_scan_operator);
    let mut min_max_predicates = Vec::new();
    if let Some(conjuncts) = node.conjuncts.as_ref() {
        for conj in conjuncts {
            match parse_min_max_conjunct(conj, &out_layout) {
                Ok(Some(pred)) => min_max_predicates.push(pred),
                Ok(None) => {}
                Err(err) => {
                    debug!(
                        "STARROCKS_SCAN_NODE node_id={} skip unsupported min/max conjunct for native pruning: {}",
                        node.node_id, err
                    );
                }
            }
        }
    }
    if !min_max_predicates.is_empty() {
        debug!(
            "STARROCKS_SCAN_NODE node_id={} parsed {} min/max predicates for native pruning",
            node.node_id,
            min_max_predicates.len()
        );
    }

    let output_slots = slot_ids.clone();
    let cfg = StarRocksScanConfig {
        db_name: sr.db_name.clone().filter(|s| !s.is_empty()),
        table_name: sr.table_name.clone().filter(|s| !s.is_empty()),
        opaqued_query_plan: sr.opaqued_query_plan.clone().unwrap_or_default(),
        properties: sr.properties.clone().unwrap_or_default(),
        ranges,
        has_more,
        required_schema,
        schema,
        slot_ids,
        query_global_dicts: std::collections::HashMap::new(),
        limit,
        batch_size,
        query_timeout,
        mem_limit,
        profile_label: Some(format!("starrocks_scan_node_id={}", node.node_id)),
        min_max_predicates,
    };

    let scan = connectors
        .create_scan_node("starrocks", ScanConfig::StarRocks(cfg))?
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
