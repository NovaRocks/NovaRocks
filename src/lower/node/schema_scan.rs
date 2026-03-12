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

use crate::common::ids::SlotId;
use crate::connector::schema::{BeSchemaTable, SchemaScanContext, SchemaScanOp, SchemaTable};
use crate::descriptors;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::node::scan::ScanNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::{Layout, chunk_schema_for_layout, schema_for_layout};
use crate::lower::node::Lowered;
use crate::novarocks_logging::warn;
use crate::{internal_service, plan_nodes, types};

use super::local_rf_waiting_set;

/// Lower a SCHEMA_SCAN_NODE to an empty `ValuesNode`.
///
/// This unblocks FE internal maintenance jobs that reference information_schema
/// while we incrementally align full schema-scan semantics.
pub(crate) fn lower_schema_scan_node(
    node: &plan_nodes::TPlanNode,
    out_layout: &Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let schema_scan = node
        .schema_scan_node
        .as_ref()
        .ok_or_else(|| "SCHEMA_SCAN_NODE missing schema_scan_node payload".to_string())?;

    if let Some(table) = SchemaTable::from_table_name(&schema_scan.table_name) {
        return match &table {
            SchemaTable::Be(BeSchemaTable::Unsupported(name)) => {
                Err(format!("unsupported be schema table {name}"))
            }
            _ => {
                let require_scan_ranges = schema_table_requires_scan_ranges(&table);
                lower_supported_schema_scan_node(
                    node,
                    out_layout,
                    desc_tbl,
                    exec_params,
                    fe_addr,
                    table,
                    require_scan_ranges,
                )
            }
        };
    }

    let chunk = if out_layout.order.is_empty() {
        Chunk::new_with_chunk_schema(
            RecordBatch::new_empty(Arc::new(Schema::empty())),
            Arc::new(ChunkSchema::empty()),
        )
    } else {
        let desc_tbl =
            desc_tbl.ok_or_else(|| "SCHEMA_SCAN_NODE requires desc_tbl for schema".to_string())?;
        let schema = schema_for_layout(desc_tbl, out_layout)?;
        let chunk_schema = chunk_schema_for_layout(desc_tbl, out_layout)?;
        Chunk::new_with_chunk_schema(RecordBatch::new_empty(schema), chunk_schema)
    };
    warn!(
        "SCHEMA_SCAN_NODE is lowered to empty values for table_name={} db={:?}",
        schema_scan.table_name, schema_scan.db
    );
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

fn schema_table_requires_scan_ranges(table: &SchemaTable) -> bool {
    matches!(
        table,
        SchemaTable::Be(
            BeSchemaTable::TabletWriteLog
                | BeSchemaTable::Txns
                | BeSchemaTable::Compactions
                | BeSchemaTable::CloudNativeCompactions
                | BeSchemaTable::Configs
                | BeSchemaTable::DatacacheMetrics
                | BeSchemaTable::Logs
                | BeSchemaTable::Tablets
                | BeSchemaTable::Threads
                | BeSchemaTable::Bvars
                | BeSchemaTable::Metrics
        )
    )
}

fn lower_supported_schema_scan_node(
    node: &plan_nodes::TPlanNode,
    out_layout: &Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    fe_addr: Option<&types::TNetworkAddress>,
    table: SchemaTable,
    require_scan_ranges: bool,
) -> Result<Lowered, String> {
    let schema_scan = node
        .schema_scan_node
        .as_ref()
        .ok_or_else(|| "SCHEMA_SCAN_NODE missing schema_scan_node payload".to_string())?;
    let output_schema = if out_layout.order.is_empty() {
        Arc::new(Schema::empty())
    } else {
        let desc_tbl =
            desc_tbl.ok_or_else(|| "SCHEMA_SCAN_NODE requires desc_tbl for schema".to_string())?;
        schema_for_layout(desc_tbl, out_layout)?
    };
    let output_chunk_schema = if out_layout.order.is_empty() {
        Arc::new(ChunkSchema::empty())
    } else {
        let desc_tbl =
            desc_tbl.ok_or_else(|| "SCHEMA_SCAN_NODE requires desc_tbl for schema".to_string())?;
        chunk_schema_for_layout(desc_tbl, out_layout)?
    };
    let output_slots = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<Vec<_>, _>>()?;
    let context = SchemaScanContext::from_thrift(schema_scan);
    let should_scan = if require_scan_ranges {
        schema_scan_selected_for_current_fragment(node.node_id, exec_params)?
    } else {
        schema_scan_selected_if_present(node.node_id, exec_params)?
    };
    let scan = ScanNode::new(Arc::new(SchemaScanOp::new(
        table,
        context,
        output_schema,
        output_chunk_schema,
        should_scan,
        fe_addr.cloned(),
    )))
    .with_node_id(node.node_id)
    .with_output_slots(output_slots)
    .with_local_rf_waiting_set(local_rf_waiting_set(node));
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: out_layout.clone(),
    })
}

fn schema_scan_selected_for_current_fragment(
    node_id: i32,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Result<bool, String> {
    let exec_params = exec_params.ok_or_else(|| {
        "SCHEMA_SCAN_NODE for be_* tables requires exec_params.per_node_scan_ranges".to_string()
    })?;
    let scan_ranges = exec_params
        .per_node_scan_ranges
        .get(&node_id)
        .ok_or_else(|| format!("missing per_node_scan_ranges for node_id={node_id}"))?;
    if scan_ranges
        .iter()
        .any(|scan_range| scan_range.has_more.unwrap_or(false))
    {
        return Err(format!(
            "SCHEMA_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node_id
        ));
    }
    if scan_ranges.is_empty() {
        return Ok(true);
    }
    Ok(scan_ranges
        .iter()
        .any(|scan_range| !scan_range.empty.unwrap_or(false)))
}

fn schema_scan_selected_if_present(
    node_id: i32,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Result<bool, String> {
    let Some(exec_params) = exec_params else {
        return Ok(true);
    };
    let Some(scan_ranges) = exec_params.per_node_scan_ranges.get(&node_id) else {
        return Ok(true);
    };
    if scan_ranges
        .iter()
        .any(|scan_range| scan_range.has_more.unwrap_or(false))
    {
        return Err(format!(
            "SCHEMA_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node_id
        ));
    }
    if scan_ranges.is_empty() {
        return Ok(true);
    }
    Ok(scan_ranges
        .iter()
        .any(|scan_range| !scan_range.empty.unwrap_or(false)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::types;

    fn schema_scan_plan_node(table_name: &str) -> plan_nodes::TPlanNode {
        plan_nodes::TPlanNode {
            node_id: 100,
            node_type: plan_nodes::TPlanNodeType::SCHEMA_SCAN_NODE,
            num_children: 0,
            limit: -1,
            row_tuples: vec![0],
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
            schema_scan_node: Some(plan_nodes::TSchemaScanNode {
                tuple_id: 0,
                table_name: table_name.to_string(),
                db: None,
                table: None,
                wild: None,
                user: None,
                ip: None,
                port: None,
                thread_id: None,
                user_ip: None,
                current_user_ident: None,
                table_id: None,
                partition_id: None,
                tablet_id: None,
                txn_id: None,
                job_id: None,
                label: None,
                type_: None,
                state: None,
                limit: None,
                log_start_ts: None,
                log_end_ts: None,
                log_level: None,
                log_pattern: None,
                log_limit: None,
                frontends: None,
                catalog_name: None,
            }),
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
        }
    }

    fn scan_exec_params(node_id: i32) -> internal_service::TPlanFragmentExecParams {
        let mut per_node_scan_ranges = BTreeMap::new();
        per_node_scan_ranges.insert(
            node_id,
            vec![internal_service::TScanRangeParams {
                scan_range: plan_nodes::TScanRange::new(
                    Option::<plan_nodes::TInternalScanRange>::None,
                    Option::<Vec<u8>>::None,
                    Option::<plan_nodes::TBrokerScanRange>::None,
                    Option::<plan_nodes::TEsScanRange>::None,
                    Option::<plan_nodes::THdfsScanRange>::None,
                    Option::<plan_nodes::TBinlogScanRange>::None,
                    Option::<plan_nodes::TBenchmarkScanRange>::None,
                ),
                volume_id: None,
                empty: Some(false),
                has_more: Some(false),
            }],
        );
        internal_service::TPlanFragmentExecParams::new(
            types::TUniqueId { hi: 1, lo: 2 },
            types::TUniqueId { hi: 3, lo: 4 },
            per_node_scan_ranges,
            BTreeMap::new(),
            Option::<Vec<crate::data_sinks::TPlanFragmentDestination>>::None,
            None::<i32>,
            None::<i32>,
            None::<bool>,
            None::<bool>,
            None::<crate::runtime_filter::TRuntimeFilterParams>,
            None::<i32>,
            None::<bool>,
            None::<BTreeMap<i32, BTreeMap<i32, Vec<internal_service::TScanRangeParams>>>>,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<Vec<internal_service::TExecDebugOption>>,
        )
    }

    #[test]
    fn lower_schema_scan_node_core_be_tables_to_scan() {
        let layout = Layout {
            order: Vec::new(),
            index: std::collections::HashMap::new(),
        };
        let table_names = [
            "be_tablet_write_log",
            "be_txns",
            "be_compactions",
            "be_bvars",
            "be_metrics",
        ];
        for table_name in table_names {
            let node = schema_scan_plan_node(table_name);
            let params = scan_exec_params(node.node_id);
            let lowered = lower_schema_scan_node(&node, &layout, None, Some(&params), None)
                .unwrap_or_else(|err| panic!("table_name={table_name} err={err}"));
            assert!(
                matches!(lowered.node.kind, ExecNodeKind::Scan(_)),
                "table_name={table_name} lower result should be scan"
            );
        }
    }

    #[test]
    fn lower_schema_scan_node_loads_to_scan() {
        let layout = Layout {
            order: Vec::new(),
            index: std::collections::HashMap::new(),
        };
        let node = schema_scan_plan_node("loads");
        let lowered = lower_schema_scan_node(&node, &layout, None, None, None)
            .expect("loads schema table should lower to scan");
        assert!(matches!(lowered.node.kind, ExecNodeKind::Scan(_)));
    }

    #[test]
    fn lower_schema_scan_node_fe_backed_tables_to_scan() {
        let layout = Layout {
            order: Vec::new(),
            index: std::collections::HashMap::new(),
        };
        for table_name in ["keywords", "tables", "columns", "fe_locks"] {
            let node = schema_scan_plan_node(table_name);
            let lowered = lower_schema_scan_node(&node, &layout, None, None, None)
                .unwrap_or_else(|err| panic!("table_name={table_name} err={err}"));
            assert!(
                matches!(lowered.node.kind, ExecNodeKind::Scan(_)),
                "table_name={table_name} lower result should be scan"
            );
        }
    }

    #[test]
    fn lower_schema_scan_node_unknown_be_tables_fail_fast() {
        let layout = Layout {
            order: Vec::new(),
            index: std::collections::HashMap::new(),
        };
        let node = schema_scan_plan_node("be_unknown_table");
        let params = scan_exec_params(node.node_id);
        let err = lower_schema_scan_node(&node, &layout, None, Some(&params), None)
            .expect_err("unknown be schema table should fail fast");
        assert!(err.contains("unsupported be schema table"), "err={err}");
    }
}
