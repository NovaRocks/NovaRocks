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

use crate::protocol::starrocks::decode::error::StarRocksFragmentDecodeError;
use crate::protocol::starrocks::decode::expr::parse_min_max_conjuncts;
use crate::protocol::starrocks::decode::layout::{
    Layout, chunk_schema_for_layout, chunk_schema_for_tuple, find_tuple_descriptor,
    layout_for_row_tuples, layout_from_slot_ids, slot_arrow_type_lookup,
    slot_display_name_from_desc,
};
use crate::protocol::starrocks::decode::node::decode::build_scan_query_global_dicts;
use crate::protocol::starrocks::decode::node::{Lowered, QueryGlobalDictMap, ScanRangeCarrier};
use crate::thrift::{descriptors, plan_nodes, types};
use novarocks::common::ids::SlotId;
use novarocks::connector::starrocks::STARROCKS_WIRE_INTERNAL_CATALOG_NAME;
use novarocks::connector::starrocks::fe_v2_meta::{
    LakeScanTabletRef, LakeTableIdentity, find_cached_table_identity_names,
    lake_scan_execution_properties,
};
use novarocks::connector::starrocks::plan_compat_starrocks_read_source;
use novarocks::exec::expr::{ExprArena, ExprNode};
use novarocks::exec::fragment::program::ScanAssignmentKind;
use novarocks::exec::node::project::ProjectNode;
use novarocks::exec::node::scan::ScanNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::exec::row_position::{
    LakeRowPositionSpec, is_lake_row_id, is_lake_rss_id, is_lake_source_id, is_lake_tablet_id,
};
use novarocks::novarocks_connectors::{
    ConnectorRegistry, LakeScanSchemaMeta, StarRocksScanConfig, StarRocksScanRange,
};
use novarocks::novarocks_logging::debug;
use novarocks::protocol::FieldPath;
use novarocks::runtime::query_options::QueryOptions;
use novarocks::runtime::scan_range::ScanRange;
use novarocks_types::QueryId;

/// Reject the retired Lake late-materialization request before lowerers build
/// provider-specific state. The row-position virtual columns are the request's
/// existing wire representation, so retain their plan-node field path.
pub(crate) fn reject_lake_late_materialization(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    node_path: FieldPath,
) -> Result<(), StarRocksFragmentDecodeError> {
    let Some(lake) = node.lake_scan_node.as_ref() else {
        return Ok(());
    };
    if lake.enable_global_late_materialization == Some(true) {
        return Err(StarRocksFragmentDecodeError::unsupported(
            node_path
                .field("lake_scan_node")
                .field("enable_global_late_materialization"),
            "LAKE_SCAN_NODE late materialization is retired; it is not part of the fragment kernel",
        ));
    }
    let slots = layout_hints
        .get(&lake.tuple_id)
        .filter(|slots| !slots.is_empty())
        .or_else(|| tuple_slots.get(&lake.tuple_id));
    let Some(slots) = slots else {
        return Ok(());
    };
    let descriptors = desc_tbl
        .and_then(|table| table.slot_descriptors.as_deref())
        .unwrap_or(&[]);
    if slots.iter().any(|slot_id| {
        descriptors
            .iter()
            .find(|slot| slot.parent == Some(lake.tuple_id) && slot.id == Some(*slot_id))
            .map(slot_display_name_from_desc)
            .is_some_and(|name| {
                is_lake_source_id(&name)
                    || is_lake_tablet_id(&name)
                    || is_lake_rss_id(&name)
                    || is_lake_row_id(&name)
            })
    }) {
        return Err(StarRocksFragmentDecodeError::unsupported(
            node_path.field("row_tuples"),
            "LAKE_SCAN_NODE late materialization is retired; row-position virtual columns are not part of the fragment kernel",
        ));
    }
    Ok(())
}

/// Lower a LAKE_SCAN_NODE plan node to a `Lowered` ExecNode.
///
/// FE-compatible lake scan lowering consumes tablet ids, versions, schema ids,
/// row-count hints, and table identity from Thrift descriptors and
/// `per_node_scan_ranges`. `TInternalScanRange` is an internal OLAP/Lake
/// protocol type, so its catalog identity is always `default_catalog`.
pub(crate) fn lower_lake_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    scan_ranges: Option<ScanRangeCarrier>,
    program_facts: Option<&crate::protocol::starrocks::decode::LakeScanProgramFacts>,
    query_id: Option<QueryId>,
    query_opts: &QueryOptions,
    arena: &mut ExprArena,
    query_global_dict_map: &QueryGlobalDictMap,
    db_name_hint: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "LAKE_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }
    let Some(lake) = node.lake_scan_node.as_ref() else {
        return Err("LAKE_SCAN_NODE missing lake_scan_node payload".to_string());
    };
    let tuple_id = lake.tuple_id;

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
            "LAKE_SCAN_NODE tuple_id={tuple_id} has empty output layout"
        ));
    }
    let original_out_layout = out_layout.clone();

    let desc_tbl = desc_tbl.ok_or_else(|| {
        format!(
            "LAKE_SCAN_NODE node_id={} requires descriptor table for schema",
            node.node_id
        )
    })?;

    // FE may request low-card dictionary slots (INT ids) via dict_string_id_to_int_ids.
    // novarocks scan currently reads storage STRING slots, then we project them back to
    // FE-requested slot ids on top of scan.
    let mut dict_int_to_string: HashMap<types::TSlotId, types::TSlotId> = HashMap::new();
    if let Some(dict_map) = lake.dict_string_id_to_int_ids.as_ref() {
        for (string_slot_id, int_slot_id) in dict_map {
            dict_int_to_string.insert(*int_slot_id, *string_slot_id);
        }
    }
    let mut scan_layout = out_layout.clone();
    if !dict_int_to_string.is_empty() {
        for (_tuple_id, slot_id) in &mut scan_layout.order {
            if let Some(string_slot_id) = dict_int_to_string.get(slot_id) {
                *slot_id = *string_slot_id;
            }
        }
        scan_layout.index = scan_layout
            .order
            .iter()
            .enumerate()
            .map(|(i, key)| (*key, i))
            .collect();
    }

    // Detect lake GLM virtual column slots (synthesized by scan runner, not in storage).
    // They must be excluded from the storage schemas so the native reader ignores them.
    let slot_desc_lookup: HashMap<
        (types::TTupleId, types::TSlotId),
        &descriptors::TSlotDescriptor,
    > = desc_tbl
        .slot_descriptors
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .filter_map(|s| Some(((s.parent?, s.id?), s)))
        .collect();

    let mut source_id_info: Option<(SlotId, arrow::datatypes::Field)> = None;
    let mut tablet_id_info: Option<(SlotId, arrow::datatypes::Field)> = None;
    let mut rss_id_info: Option<(SlotId, arrow::datatypes::Field)> = None;
    let mut row_id_info: Option<(SlotId, arrow::datatypes::Field)> = None;

    // Separate virtual cols from storage cols; produce a storage-only order
    let mut storage_order: Vec<(types::TTupleId, types::TSlotId)> =
        Vec::with_capacity(scan_layout.order.len());
    for entry @ (tuple_id_raw, slot_id_raw) in &scan_layout.order {
        let found_virtual = 'check: {
            let Some(s) = slot_desc_lookup.get(&(*tuple_id_raw, *slot_id_raw)) else {
                break 'check false;
            };
            let Ok(slot_id) = SlotId::try_from(*slot_id_raw) else {
                break 'check false;
            };
            let Some(slot_type) = s.slot_type.as_ref() else {
                break 'check false;
            };
            let Some(arrow_type) =
                crate::protocol::starrocks::decode::type_lowering::arrow_type_from_desc(slot_type)
            else {
                break 'check false;
            };
            let name = slot_display_name_from_desc(s);
            let nullable = s.is_nullable.unwrap_or(true);
            if is_lake_source_id(&name) {
                source_id_info = Some((
                    slot_id,
                    arrow::datatypes::Field::new(&name, arrow_type, nullable),
                ));
                break 'check true;
            }
            if is_lake_tablet_id(&name) {
                tablet_id_info = Some((
                    slot_id,
                    arrow::datatypes::Field::new(&name, arrow_type, nullable),
                ));
                break 'check true;
            }
            if is_lake_rss_id(&name) {
                rss_id_info = Some((
                    slot_id,
                    arrow::datatypes::Field::new(&name, arrow_type, nullable),
                ));
                break 'check true;
            }
            if is_lake_row_id(&name) {
                row_id_info = Some((
                    slot_id,
                    arrow::datatypes::Field::new(&name, arrow_type, nullable),
                ));
                break 'check true;
            }
            false
        };
        if !found_virtual {
            storage_order.push(*entry);
        }
    }

    let lake_row_position_spec = match (source_id_info, tablet_id_info, rss_id_info, row_id_info) {
        (None, None, None, None) => None,
        (
            Some((source_id_slot, source_id_field)),
            Some((tablet_id_slot, tablet_id_field)),
            Some((rss_id_slot, rss_id_field)),
            Some((row_id_slot, row_id_field)),
        ) => Some(LakeRowPositionSpec {
            source_id_slot,
            tablet_id_slot,
            rss_id_slot,
            row_id_slot,
            source_id_field,
            tablet_id_field,
            rss_id_field,
            row_id_field,
        }),
        _ => {
            return Err(format!(
                "LAKE_SCAN_NODE node_id={} lake row position slots must all be present together \
                (_source_id_/_tablet_id_/_rss_id_/_row_id_)",
                node.node_id
            ));
        }
    };

    if lake_row_position_spec.is_some() {
        return Err(format!(
            "LAKE_SCAN_NODE node_id={} late materialization is retired",
            node.node_id
        ));
    }

    if lake_row_position_spec.is_some() {
        // Rebuild scan_layout with virtual cols removed
        scan_layout.order = storage_order;
        scan_layout.index = scan_layout
            .order
            .iter()
            .enumerate()
            .map(|(i, key)| (*key, i))
            .collect();
    }

    let scan_output_chunk_schema = chunk_schema_for_layout(desc_tbl, &scan_layout)?;
    // When lake GLM virtual cols are present, build required_chunk_schema from the storage-only
    // layout; otherwise fall back to the full tuple layout.
    let required_chunk_schema = if lake_row_position_spec.is_some() {
        scan_output_chunk_schema.clone()
    } else {
        chunk_schema_for_tuple(desc_tbl, tuple_id)?
    };
    let scan_output_schema = scan_output_chunk_schema.arrow_schema_ref();
    if !scan_output_chunk_schema.slot_ids().is_empty()
        && scan_output_chunk_schema.slot_ids().len() != scan_output_schema.fields().len()
    {
        return Err(format!(
            "LAKE_SCAN_NODE output layout/schema mismatch: layout_len={}, schema_len={}",
            scan_output_chunk_schema.slot_ids().len(),
            scan_output_schema.fields().len()
        ));
    }

    let Some(scan_ranges) = scan_ranges else {
        return Err("LAKE_SCAN_NODE requires exec_params.per_node_scan_ranges".to_string());
    };
    let (assignment_kind, assignment_ranges) = scan_ranges
        .get(node.node_id)
        .ok_or_else(|| format!("missing typed scan assignment for node_id={}", node.node_id))?;
    if assignment_kind != ScanAssignmentKind::StarRocksTablet {
        return Err(format!(
            "LAKE_SCAN_NODE node_id={} expected StarRocksTablet assignment, got {assignment_kind:?}",
            node.node_id,
        ));
    }
    let program_facts = program_facts.ok_or_else(|| {
        format!(
            "LAKE_SCAN_NODE node_id={} missing normalized program facts",
            node.node_id
        )
    })?;

    let mut ranges = Vec::new();
    let mut refs = Vec::new();
    let internal_db_name = program_facts.db_name.clone();
    let internal_table_name = program_facts.table_name.clone();
    let mut has_more = false;
    for p in assignment_ranges {
        if p.empty.unwrap_or(false) {
            if p.has_more.unwrap_or(false) {
                has_more = true;
            }
            continue;
        }
        let ScanRange::StarRocksTablet(internal) = &p.range else {
            return Err(format!(
                "LAKE_SCAN_NODE node_id={} assignment contains non-tablet range",
                node.node_id
            ));
        };
        let version = internal.version;
        if version <= 0 {
            return Err(format!(
                "invalid non-positive tablet version for tablet_id={}: {}",
                internal.tablet_id, version
            ));
        }
        let partition_id = internal.partition_id;

        refs.push(LakeScanTabletRef {
            tablet_id: internal.tablet_id,
            partition_id,
            version,
        });
        ranges.push(StarRocksScanRange {
            tablet_id: internal.tablet_id,
            partition_id: Some(internal.partition_id),
            version: Some(version),
        });
    }
    if has_more {
        return Err(format!(
            "LAKE_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node.node_id
        ));
    }
    let tuple_desc = find_tuple_descriptor(desc_tbl, tuple_id)?;
    let table_id_from_tuple = tuple_desc
        .table_id
        .ok_or_else(|| format!("LAKE_SCAN_NODE tuple_id={} missing table_id", tuple_id))?;
    let table_descs = desc_tbl
        .table_descriptors
        .as_ref()
        .ok_or_else(|| "LAKE_SCAN_NODE missing table_descriptors in desc_tbl".to_string())?;
    let table_desc = table_descs
        .iter()
        .find(|t| t.id == table_id_from_tuple)
        .ok_or_else(|| {
            format!(
                "LAKE_SCAN_NODE missing table descriptor for table_id={}",
                table_id_from_tuple
            )
        })?;

    let mut db_name = if table_desc.db_name.trim().is_empty() {
        if let Some(name) = db_name_hint
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
        {
            name
        } else if let Some(name) = internal_db_name.clone() {
            name
        } else {
            "__unknown_db__".to_string()
        }
    } else {
        table_desc.db_name.clone()
    };
    let mut table_name = if table_desc.table_name.trim().is_empty() {
        internal_table_name.unwrap_or_else(|| "__unknown_table__".to_string())
    } else {
        table_desc.table_name.trim().to_string()
    };

    let schema_key = lake
        .schema_key
        .as_ref()
        .ok_or_else(|| "LAKE_SCAN_NODE missing schema_key".to_string())?;
    let db_id = schema_key
        .db_id
        .ok_or_else(|| "LAKE_SCAN_NODE schema_key missing db_id".to_string())?;
    let table_id = schema_key
        .table_id
        .ok_or_else(|| "LAKE_SCAN_NODE schema_key missing table_id".to_string())?;
    let schema_id = schema_key
        .schema_id
        .ok_or_else(|| "LAKE_SCAN_NODE schema_key missing schema_id".to_string())?;

    if table_id != table_desc.id {
        return Err(format!(
            "LAKE_SCAN_NODE schema_key.table_id={} mismatches descriptor table_id={}",
            table_id, table_desc.id
        ));
    }
    let catalog = STARROCKS_WIRE_INTERNAL_CATALOG_NAME.to_string();
    if (db_name == "__unknown_db__" || table_name == "__unknown_table__")
        && let Some((cached_db_name, cached_table_name)) =
            find_cached_table_identity_names(&catalog, db_id, table_id)
    {
        if db_name == "__unknown_db__" {
            db_name = cached_db_name;
        }
        if table_name == "__unknown_table__" {
            table_name = cached_table_name;
        }
    }
    let table_identity = internal_lake_table_identity(
        db_name.clone(),
        table_name.clone(),
        db_id,
        table_id,
        schema_id,
    );

    let query_id = query_id.ok_or_else(|| "LAKE_SCAN_NODE missing query id".to_string())?;
    let starlet_metadata_provider =
        external_dependencies.and_then(|draft| draft.starlet_metadata_provider());
    let properties = lake_scan_execution_properties(
        Some(query_id),
        &table_identity,
        &refs,
        starlet_metadata_provider.as_deref(),
    )
    .map_err(|e| {
            format!(
                "LAKE_SCAN_NODE resolve tablet paths failed for catalog={} db_name={} table_name={} db_id={} table_id={} schema_id={}: {}",
                table_identity.catalog,
                table_identity.db_name,
                table_identity.table_name,
                table_identity.db_id,
                table_identity.table_id,
                table_identity.schema_id,
                e
            )
        })?;
    let limit = (node.limit >= 0).then_some(node.limit as usize);
    let batch_size = query_opts.batch_size().or(Some(4096));
    let query_timeout = query_opts.query_timeout();
    let mem_limit = query_opts.exec_mem_limit().filter(|v| *v > 0);
    let connector_io_tasks_per_scan_operator = query_opts.connector_io_tasks_per_scan_operator();
    let mut min_max_predicates = Vec::new();
    if let Some(conjuncts) = node.conjuncts.as_ref() {
        for conj in conjuncts {
            for pred in parse_min_max_conjuncts(conj, &out_layout)? {
                min_max_predicates.push(pred);
            }
        }
    }
    debug!(
        "LAKE_SCAN_NODE node_id={} resolved {} tablets via Starlet AddShard cache",
        node.node_id,
        refs.len()
    );
    let compat_output_slots = original_out_layout
        .order
        .iter()
        .map(|(tuple_id, slot_id)| {
            let name = slot_desc_lookup
                .get(&(*tuple_id, *slot_id))
                .map(|slot| slot_display_name_from_desc(slot))
                .unwrap_or_else(|| "<missing>".to_string());
            format!("{tuple_id}:{slot_id}:{name}")
        })
        .collect::<Vec<_>>();
    let compat_row_position_slots = lake_row_position_spec.as_ref().map(|spec| {
        [
            spec.source_id_slot.as_u32(),
            spec.tablet_id_slot.as_u32(),
            spec.rss_id_slot.as_u32(),
            spec.row_id_slot.as_u32(),
        ]
    });
    eprintln!(
        "compat_scan node_type=LAKE_SCAN_NODE node_id={} tuple_id={} output_slots={:?} row_position_slots={:?}",
        node.node_id, tuple_id, compat_output_slots, compat_row_position_slots
    );

    let topn_filter_column_map = HashMap::new();

    let cfg = StarRocksScanConfig {
        db_name: normalize_optional_table_name(&db_name, "__unknown_db__"),
        table_name: normalize_optional_table_name(&table_name, "__unknown_table__"),
        properties,
        ranges,
        has_more,
        required_chunk_schema,
        output_chunk_schema: scan_output_chunk_schema.clone(),
        query_global_dicts: build_scan_query_global_dicts(
            scan_output_chunk_schema.slot_ids(),
            query_global_dict_map,
        )?,
        limit,
        batch_size,
        query_timeout,
        mem_limit,
        profile_label: Some(format!("lake_scan_node_id={}", node.node_id)),
        min_max_predicates,
        lake_schema_meta: Some(LakeScanSchemaMeta {
            db_id,
            table_id,
            schema_id,
            fe_addr: external_dependencies.and_then(|draft| draft.frontend_endpoint().cloned()),
            query_id: Some(novarocks_types::UniqueId::new(
                query_id.high(),
                query_id.low(),
            )),
            native_tablet_schema: None,
            native_column_hints: None,
            table_schema_provider: external_dependencies
                .and_then(|draft| draft.table_schema_provider()),
            storage_metadata_provider: external_dependencies
                .and_then(|draft| draft.storage_metadata_provider()),
        }),
        deferred_lake_resolution: None,
        topn_filter_column_map,
    };

    let source = plan_compat_starrocks_read_source(query_id, node.node_id, cfg, query_opts)
        .map_err(|error| error.to_string())?;
    scan_ranges.capture(
        node.node_id,
        novarocks::exec::node::scan::BoundScanRanges::None,
    );
    let scan = ScanNode::new(source)
        .with_node_id(node.node_id)
        .with_output_chunk_schema(scan_output_chunk_schema.clone())
        .with_limit(limit)
        .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
        .with_accept_empty_scan_ranges(true)
        .with_lake_row_position(lake_row_position_spec);
    let scan_lowered = Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: scan_layout,
    };

    // Skip the dict-expansion projection if there's nothing to remap,
    // or if lake GLM virtual cols were removed (that difference is not dict-related).
    if dict_int_to_string.is_empty() {
        return Ok(scan_lowered);
    }

    let slot_types = slot_arrow_type_lookup(desc_tbl)?;
    let projected_output_chunk_schema = chunk_schema_for_layout(desc_tbl, &original_out_layout)?;

    let mut exprs = Vec::with_capacity(original_out_layout.order.len());
    let mut expr_slot_ids = Vec::with_capacity(original_out_layout.order.len());
    for (_tuple_id, output_slot_id) in &original_out_layout.order {
        let source_slot_id = dict_int_to_string
            .get(output_slot_id)
            .copied()
            .unwrap_or(*output_slot_id);
        let source_slot = SlotId::try_from(source_slot_id)?;
        let output_slot = SlotId::try_from(*output_slot_id)?;
        let source_tuple_id =
            resolve_layout_slot_tuple_id(&scan_lowered.layout, source_slot_id, "LAKE_SCAN_NODE")?;
        let data_type = slot_types
            .get(&(source_tuple_id, source_slot_id))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "LAKE_SCAN_NODE missing source slot type for tuple_id={} slot_id={}",
                    source_tuple_id, source_slot_id
                )
            })?;
        let expr = arena.push_typed(ExprNode::SlotId(source_slot), data_type);
        exprs.push(expr);
        expr_slot_ids.push(output_slot);
    }

    let output_indices: Vec<usize> = (0..exprs.len()).collect();
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(scan_lowered.node),
                node_id: node.node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids,
                expr_slot_schemas: None,
                output_indices: Some(output_indices),
                output_chunk_schema: projected_output_chunk_schema,
            }),
        },
        layout: original_out_layout,
    })
}

fn resolve_layout_slot_tuple_id(
    layout: &Layout,
    slot_id: types::TSlotId,
    context: &str,
) -> Result<types::TTupleId, String> {
    let mut matches = layout
        .order
        .iter()
        .filter_map(|(tuple_id, layout_slot_id)| (*layout_slot_id == slot_id).then_some(*tuple_id));
    let tuple_id = matches
        .next()
        .ok_or_else(|| format!("{context} missing slot_id={slot_id} in layout"))?;
    if matches.next().is_some() {
        return Err(format!(
            "{context} ambiguous slot_id={} across multiple tuples",
            slot_id
        ));
    }
    Ok(tuple_id)
}

fn normalize_optional_table_name(name: &str, unknown_sentinel: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case(unknown_sentinel) {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub(super) fn internal_lake_table_identity(
    db_name: String,
    table_name: String,
    db_id: i64,
    table_id: i64,
    schema_id: i64,
) -> LakeTableIdentity {
    LakeTableIdentity {
        catalog: STARROCKS_WIRE_INTERNAL_CATALOG_NAME.to_string(),
        db_name,
        table_name,
        db_id,
        table_id,
        schema_id,
    }
}
