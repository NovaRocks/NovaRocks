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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::TimeUnit;
use chrono::NaiveDate;

use crate::common::ids::SlotId;
use crate::connector::starrocks::fe_v2_meta::LakeTabletPartitionRef;
use crate::connector::starrocks::sink::partition_key::{
    PartitionKeySource, PartitionKeyValue, PartitionMode, PartitionRoutingEntry,
    build_partition_key_arrays, build_partition_key_source, build_row_partition_key,
    build_slot_name_map, compare_partition_key_vectors, parse_partition_boundary_key,
    parse_partition_in_keys, partition_key_source_len, resolve_slot_ids_by_names,
    validate_partition_key_length,
};
use crate::exec::chunk::Chunk;
use crate::{data_sinks, descriptors, exprs, types};

const LOAD_OP_COLUMN: &str = "__op";

#[derive(Clone)]
pub(crate) struct RowRoutingPlan {
    pub(crate) tablet_ids: Vec<i64>,
    pub(crate) tablet_idx_by_id: HashMap<i64, usize>,
    pub(crate) distributed_slot_ids: Vec<SlotId>,
    pub(crate) partition_key_source: PartitionKeySource,
    pub(crate) partition_key_len: usize,
    pub(crate) partition_mode: PartitionMode,
    pub(crate) partitions: Vec<PartitionRoutingEntry>,
}

pub(crate) struct SinkRouting {
    pub(crate) commit_infos: Vec<types::TTabletCommitInfo>,
    pub(crate) refs: Vec<LakeTabletPartitionRef>,
    pub(crate) tablet_to_partition: BTreeMap<i64, i64>,
    pub(crate) row_routing: RowRoutingPlan,
}

pub(crate) fn build_sink_routing(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<SinkRouting, String> {
    let candidate_index_ids = sink
        .schema
        .indexes
        .iter()
        .filter(|idx| idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id) == schema_id)
        .map(|idx| idx.id)
        .collect::<HashSet<_>>();
    if candidate_index_ids.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK cannot resolve routing index for schema_id={schema_id}"
        ));
    }
    build_sink_routing_with_candidates(sink, schema_id, candidate_index_ids, output_exprs)
}

pub(crate) fn build_sink_routing_for_index_id(
    sink: &data_sinks::TOlapTableSink,
    index_id: i64,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<SinkRouting, String> {
    if index_id <= 0 {
        return Err(format!(
            "OLAP_TABLE_SINK cannot build routing for non-positive index_id={index_id}"
        ));
    }
    let mut candidate_index_ids = HashSet::new();
    candidate_index_ids.insert(index_id);
    build_sink_routing_with_candidates(sink, schema_id, candidate_index_ids, output_exprs)
}

fn build_sink_routing_with_candidates(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
    candidate_index_ids: HashSet<i64>,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<SinkRouting, String> {
    let table_name = sink
        .table_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or("<unknown_table>");
    let db_name = sink
        .db_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or("<unknown_db>");

    let mut valid_backend_ids = HashSet::new();
    for node in &sink.nodes_info.nodes {
        valid_backend_ids.insert(node.id);
    }
    if valid_backend_ids.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK nodes_info is empty for {}.{} (table_id={})",
            db_name, table_name, sink.table_id
        ));
    }

    let mut tablet_to_backend = HashMap::new();
    for loc in &sink.location.tablets {
        let backend_id = *loc.node_ids.first().ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK location has empty node_ids for tablet {} on {}.{} (table_id={})",
                loc.tablet_id, db_name, table_name, sink.table_id
            )
        })?;
        if !valid_backend_ids.contains(&backend_id) {
            return Err(format!(
                "OLAP_TABLE_SINK location backend {} for tablet {} is not found in nodes_info on {}.{} (table_id={})",
                backend_id, loc.tablet_id, db_name, table_name, sink.table_id
            ));
        }
        if let Some(prev) = tablet_to_backend.insert(loc.tablet_id, backend_id)
            && prev != backend_id
        {
            return Err(format!(
                "OLAP_TABLE_SINK location backend mismatch for tablet {}: {} vs {} on {}.{} (table_id={})",
                loc.tablet_id, prev, backend_id, db_name, table_name, sink.table_id
            ));
        }
    }

    let partition_map = collect_tablet_partition_map(&sink.partition, &sink.location)?;
    let row_routing = build_row_routing_plan(sink, schema_id, &candidate_index_ids, output_exprs)?;

    let mut commit_infos = Vec::with_capacity(row_routing.tablet_ids.len());
    let mut refs = Vec::with_capacity(row_routing.tablet_ids.len());
    for tablet_id in &row_routing.tablet_ids {
        let backend_id = *tablet_to_backend.get(tablet_id).ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK location missing mapping for tablet {} on {}.{} (table_id={})",
                tablet_id, db_name, table_name, sink.table_id
            )
        })?;
        commit_infos.push(types::TTabletCommitInfo::new(
            *tablet_id,
            backend_id,
            Option::<Vec<String>>::None,
            Option::<Vec<String>>::None,
            Option::<Vec<i64>>::None,
        ));
        if !partition_map.contains_key(tablet_id) {
            return Err(format!(
                "missing partition id for tablet {} while building sink refs",
                tablet_id
            ));
        }
        refs.push(LakeTabletPartitionRef {
            tablet_id: *tablet_id,
        });
    }

    Ok(SinkRouting {
        commit_infos,
        refs,
        tablet_to_partition: partition_map,
        row_routing,
    })
}

fn build_row_routing_plan(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
    candidate_index_ids: &HashSet<i64>,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<RowRoutingPlan, String> {
    let visible_partitions = sink
        .partition
        .partitions
        .iter()
        .filter(|part| !part.is_shadow_partition.unwrap_or(false))
        .collect::<Vec<_>>();

    if candidate_index_ids.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK cannot resolve routing index for schema_id={schema_id}"
        ));
    }

    let output_expr_slot_name_map = build_output_expr_slot_name_map(sink, schema_id, output_exprs)?;
    let slot_name_overrides = if output_expr_slot_name_map.is_empty() {
        None
    } else {
        Some(&output_expr_slot_name_map)
    };
    let output_expr_slot_id_overrides =
        build_output_expr_slot_id_overrides(&sink.schema.slot_descs, slot_name_overrides)?;
    let slot_id_overrides = if output_expr_slot_id_overrides.is_empty() {
        None
    } else {
        Some(&output_expr_slot_id_overrides)
    };
    let distributed_slot_ids = resolve_distributed_slot_ids(sink, slot_name_overrides)?;
    if visible_partitions.is_empty() {
        return build_location_only_row_routing(sink, distributed_slot_ids);
    }
    let routing_partitions = visible_partitions;

    let mut partition_key_source =
        build_partition_key_source(sink, slot_name_overrides, slot_id_overrides)?;
    let mut partition_key_len = partition_key_source_len(&partition_key_source);

    let has_any_in_keys = routing_partitions
        .iter()
        .any(|part| part.in_keys.as_ref().is_some_and(|v| !v.is_empty()));
    let has_any_range_bound = routing_partitions.iter().any(|part| {
        part.start_keys
            .as_ref()
            .map_or(false, |keys| !keys.is_empty())
            || part
                .end_keys
                .as_ref()
                .map_or(false, |keys| !keys.is_empty())
            || part.start_key.is_some()
            || part.end_key.is_some()
    });
    let partition_mode = if partition_key_len == 0 || (!has_any_in_keys && !has_any_range_bound) {
        PartitionMode::Unpartitioned
    } else if has_any_in_keys {
        if routing_partitions
            .iter()
            .any(|part| !part.in_keys.as_ref().is_some_and(|v| !v.is_empty()))
        {
            return Err(
                "OLAP_TABLE_SINK mixed list/range partitions are not supported in row routing"
                    .to_string(),
            );
        }
        PartitionMode::List
    } else {
        PartitionMode::Range
    };
    if matches!(partition_mode, PartitionMode::Unpartitioned) {
        partition_key_source = PartitionKeySource::None;
        partition_key_len = 0;
    }

    let mut partitions = Vec::with_capacity(routing_partitions.len());
    let mut tablet_ids = BTreeSet::new();
    for partition in routing_partitions {
        let index = partition
            .indexes
            .iter()
            .find(|idx| candidate_index_ids.contains(&idx.index_id))
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK partition {} has no matching index for schema_id={} (candidate_index_ids={:?})",
                    partition.id, schema_id, candidate_index_ids
                )
            })?;
        if index.tablet_ids.is_empty() {
            return Err(format!(
                "OLAP_TABLE_SINK partition {} index {} has empty tablet_ids",
                partition.id, index.index_id
            ));
        }

        let start_key = parse_partition_boundary_key(
            partition.start_keys.as_deref(),
            partition.start_key.as_ref(),
        )?;
        let end_key = parse_partition_boundary_key(
            partition.end_keys.as_deref(),
            partition.end_key.as_ref(),
        )?;
        let in_keys = parse_partition_in_keys(partition.in_keys.as_deref())?;

        validate_partition_key_length(
            partition.id,
            partition_key_len,
            start_key.as_deref(),
            end_key.as_deref(),
            &in_keys,
        )?;

        match partition_mode {
            PartitionMode::Unpartitioned => {}
            PartitionMode::Range => {
                if end_key.is_none() {
                    return Err(format!(
                        "OLAP_TABLE_SINK range partition {} missing end key",
                        partition.id
                    ));
                }
            }
            PartitionMode::List => {
                if in_keys.is_empty() {
                    return Err(format!(
                        "OLAP_TABLE_SINK list partition {} has empty in_keys",
                        partition.id
                    ));
                }
            }
        }

        for tablet_id in &index.tablet_ids {
            tablet_ids.insert(*tablet_id);
        }
        partitions.push(PartitionRoutingEntry {
            partition_id: partition.id,
            tablet_ids: index.tablet_ids.clone(),
            start_key,
            end_key,
            in_keys,
        });
    }

    if partitions.is_empty() {
        return Err("OLAP_TABLE_SINK resolved empty visible partitions".to_string());
    }
    if tablet_ids.is_empty() {
        return Err("OLAP_TABLE_SINK resolved empty tablet routing".to_string());
    }

    let tablet_ids = tablet_ids.into_iter().collect::<Vec<_>>();
    let mut tablet_idx_by_id = HashMap::with_capacity(tablet_ids.len());
    for (idx, tablet_id) in tablet_ids.iter().enumerate() {
        tablet_idx_by_id.insert(*tablet_id, idx);
    }
    Ok(RowRoutingPlan {
        tablet_ids,
        tablet_idx_by_id,
        distributed_slot_ids,
        partition_key_source,
        partition_key_len,
        partition_mode,
        partitions,
    })
}

fn build_location_only_row_routing(
    sink: &data_sinks::TOlapTableSink,
    distributed_slot_ids: Vec<SlotId>,
) -> Result<RowRoutingPlan, String> {
    let mut tablet_ids = BTreeSet::new();
    for loc in &sink.location.tablets {
        if loc.tablet_id > 0 {
            tablet_ids.insert(loc.tablet_id);
        }
    }
    if tablet_ids.is_empty() {
        return Err(
            "OLAP_TABLE_SINK has no visible partitions and no tablets in location metadata"
                .to_string(),
        );
    }
    let tablet_ids = tablet_ids.into_iter().collect::<Vec<_>>();
    let mut tablet_idx_by_id = HashMap::with_capacity(tablet_ids.len());
    for (idx, tablet_id) in tablet_ids.iter().enumerate() {
        tablet_idx_by_id.insert(*tablet_id, idx);
    }
    Ok(RowRoutingPlan {
        tablet_ids: tablet_ids.clone(),
        tablet_idx_by_id,
        distributed_slot_ids,
        partition_key_source: PartitionKeySource::None,
        partition_key_len: 0,
        partition_mode: PartitionMode::Unpartitioned,
        partitions: vec![PartitionRoutingEntry {
            partition_id: 0,
            tablet_ids,
            start_key: None,
            end_key: None,
            in_keys: Vec::new(),
        }],
    })
}

fn collect_tablet_partition_map(
    partition: &descriptors::TOlapTablePartitionParam,
    location: &descriptors::TOlapTableLocationParam,
) -> Result<BTreeMap<i64, i64>, String> {
    let mut visible_map = BTreeMap::new();
    for part in &partition.partitions {
        if part.is_shadow_partition.unwrap_or(false) {
            continue;
        }
        for index in &part.indexes {
            for tablet_id in &index.tablet_ids {
                if let Some(existing) = visible_map.insert(*tablet_id, part.id)
                    && existing != part.id
                {
                    return Err(format!(
                        "tablet {} appears in multiple partitions: {} vs {}",
                        tablet_id, existing, part.id
                    ));
                }
            }
        }
    }
    if !visible_map.is_empty() {
        return Ok(visible_map);
    }

    let mut fallback_map = BTreeMap::new();
    for loc in &location.tablets {
        if loc.tablet_id > 0 {
            fallback_map.entry(loc.tablet_id).or_insert(0);
        }
    }
    if fallback_map.is_empty() {
        return Err(
            "OLAP_TABLE_SINK cannot resolve tablet-to-partition mapping from partition/location metadata"
                .to_string(),
        );
    }
    Ok(fallback_map)
}

fn resolve_distributed_slot_ids(
    sink: &data_sinks::TOlapTableSink,
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
) -> Result<Vec<SlotId>, String> {
    let distributed_columns = sink
        .partition
        .distributed_columns
        .as_ref()
        .map(|v| {
            v.iter()
                .map(|col| col.trim())
                .filter(|col| !col.is_empty())
                .map(|col| col.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if distributed_columns.is_empty() {
        return Ok(Vec::new());
    }
    resolve_slot_ids_by_names(
        &sink.schema.slot_descs,
        &distributed_columns,
        "distributed columns",
        slot_name_overrides,
    )
}

fn build_output_expr_slot_name_map(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<HashMap<String, SlotId>, String> {
    let Some(output_exprs) = output_exprs else {
        return Ok(HashMap::new());
    };
    if output_exprs.is_empty() {
        return Ok(HashMap::new());
    }

    let mut slot_map = HashMap::new();
    let named_slot_count = sink
        .schema
        .slot_descs
        .iter()
        .filter(|slot| {
            slot.col_name
                .as_deref()
                .map(str::trim)
                .is_some_and(|name| !name.is_empty())
        })
        .count();
    let skip_load_op = output_exprs.len() < named_slot_count;
    // Prefer slot descriptor order because it matches sink output tuple in DELETE/partial plans.
    // Only consume one output expr after we find a valid column slot, so hidden/empty slots
    // cannot shift all subsequent bindings.
    let mut expr_iter = output_exprs.iter();
    for slot_desc in &sink.schema.slot_descs {
        let Some(column_name) = slot_desc
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_ascii_lowercase)
        else {
            continue;
        };
        if skip_load_op && column_name == LOAD_OP_COLUMN {
            continue;
        }
        let Some(expr) = expr_iter.next() else {
            break;
        };
        let Some(root) = expr.nodes.first() else {
            continue;
        };
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = root.slot_ref.as_ref() else {
            continue;
        };
        let slot_id = SlotId::try_from(slot_ref.slot_id)?;
        slot_map.insert(column_name, slot_id);
    }
    if !slot_map.is_empty() {
        return Ok(slot_map);
    }

    let index = sink
        .schema
        .indexes
        .iter()
        .find(|idx| idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id) == schema_id)
        .ok_or_else(|| {
            format!("OLAP_TABLE_SINK cannot resolve schema index for schema_id={schema_id}")
        })?;
    let mut column_names = if let Some(param) = index.column_param.as_ref() {
        param
            .columns
            .iter()
            .map(|c| c.column_name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| name.to_ascii_lowercase())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if column_names.is_empty() {
        column_names = index
            .columns
            .iter()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| name.to_ascii_lowercase())
            .collect::<Vec<_>>();
    }
    if column_names.is_empty() {
        return Ok(HashMap::new());
    }

    for (column_name, expr) in column_names.iter().zip(output_exprs.iter()) {
        let Some(root) = expr.nodes.first() else {
            continue;
        };
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = root.slot_ref.as_ref() else {
            continue;
        };
        let slot_id = SlotId::try_from(slot_ref.slot_id)?;
        slot_map.insert(column_name.clone(), slot_id);
    }
    Ok(slot_map)
}

fn build_output_expr_slot_id_overrides(
    slot_descs: &[descriptors::TSlotDescriptor],
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
) -> Result<HashMap<SlotId, SlotId>, String> {
    let Some(slot_name_overrides) = slot_name_overrides else {
        return Ok(HashMap::new());
    };
    if slot_name_overrides.is_empty() {
        return Ok(HashMap::new());
    }

    let schema_slot_by_name = build_slot_name_map(slot_descs)?;
    let mut slot_id_overrides = HashMap::new();
    for (column_name, schema_slot_id) in schema_slot_by_name {
        let Some(output_slot_id) = slot_name_overrides.get(&column_name).copied() else {
            continue;
        };
        if output_slot_id != schema_slot_id {
            slot_id_overrides.insert(schema_slot_id, output_slot_id);
        }
    }
    Ok(slot_id_overrides)
}

pub(crate) fn route_chunk_rows(
    row_routing: &RowRoutingPlan,
    chunk: &Chunk,
    next_random_hash: &mut u32,
) -> Result<Vec<Vec<u32>>, String> {
    if row_routing.tablet_ids.is_empty() {
        return Err("OLAP_TABLE_SINK has empty tablet routing".to_string());
    }
    if row_routing.partitions.is_empty() {
        return Err("OLAP_TABLE_SINK has empty partition routing".to_string());
    }

    let mut per_tablet = vec![Vec::<u32>::new(); row_routing.tablet_ids.len()];
    let mut dist_columns = Vec::with_capacity(row_routing.distributed_slot_ids.len());
    for slot_id in &row_routing.distributed_slot_ids {
        let col = chunk.column_by_slot_id(*slot_id).map_err(|e| {
            format!(
                "OLAP_TABLE_SINK distributed slot {} is not available in chunk: {}",
                slot_id, e
            )
        })?;
        dist_columns.push((*slot_id, col));
    }

    let partition_key_arrays =
        build_partition_key_arrays(&row_routing.partition_key_source, chunk)?;
    for row in 0..chunk.len() {
        let hash = if dist_columns.is_empty() {
            let h = *next_random_hash;
            *next_random_hash = next_random_hash.wrapping_add(1);
            h
        } else {
            let mut hash = 0_u32;
            for (slot_id, array) in &dist_columns {
                hash = crc32_hash_array_value(array.as_ref(), row, hash).map_err(|e| {
                    format!(
                        "OLAP_TABLE_SINK hash distributed column failed: slot_id={} row={} error={}",
                        slot_id, row, e
                    )
                })?;
            }
            hash
        };

        let row_key = build_row_partition_key(&partition_key_arrays, row)?;
        if row_key.len() != row_routing.partition_key_len {
            return Err(format!(
                "OLAP_TABLE_SINK row partition key length mismatch: expected={} actual={}",
                row_routing.partition_key_len,
                row_key.len()
            ));
        }

        let partition = select_partition_for_row(row_routing, &row_key, hash)?;
        if partition.tablet_ids.is_empty() {
            return Err(format!(
                "OLAP_TABLE_SINK partition {} has empty tablet_ids in routing",
                partition.partition_id
            ));
        }
        let tablet_id = partition.tablet_ids[(hash as usize) % partition.tablet_ids.len()];
        let target_idx = *row_routing
            .tablet_idx_by_id
            .get(&tablet_id)
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK missing tablet routing index for tablet {}",
                    tablet_id
                )
            })?;
        per_tablet[target_idx]
            .push(u32::try_from(row).map_err(|_| format!("row index overflow: {row}"))?);
    }

    Ok(per_tablet)
}

fn select_partition_for_row<'a>(
    row_routing: &'a RowRoutingPlan,
    row_key: &[PartitionKeyValue],
    hash: u32,
) -> Result<&'a PartitionRoutingEntry, String> {
    let mut candidates = Vec::new();
    match row_routing.partition_mode {
        PartitionMode::Unpartitioned => {
            candidates.extend(0..row_routing.partitions.len());
        }
        PartitionMode::List => {
            for (idx, partition) in row_routing.partitions.iter().enumerate() {
                if partition.in_keys.iter().any(|key| {
                    match compare_partition_key_vectors(row_key, key) {
                        Ok(ordering) => ordering == Ordering::Equal,
                        Err(_) => false,
                    }
                }) {
                    candidates.push(idx);
                }
            }
        }
        PartitionMode::Range => {
            for (idx, partition) in row_routing.partitions.iter().enumerate() {
                let Some(end_key) = partition.end_key.as_ref() else {
                    return Err(format!(
                        "OLAP_TABLE_SINK range partition {} missing end key",
                        partition.partition_id
                    ));
                };
                let lt_end = compare_partition_key_vectors(row_key, end_key)? == Ordering::Less;
                if !lt_end {
                    continue;
                }
                let ge_start = match partition.start_key.as_ref() {
                    None => true,
                    Some(start) => compare_partition_key_vectors(row_key, start)? != Ordering::Less,
                };
                if ge_start {
                    candidates.push(idx);
                }
            }
        }
    }

    if candidates.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK row does not match any partition (mode={:?})",
            row_routing.partition_mode
        ));
    }
    let selected_idx = candidates[(hash as usize) % candidates.len()];
    row_routing
        .partitions
        .get(selected_idx)
        .ok_or_else(|| format!("invalid partition routing index: {}", selected_idx))
}

fn format_date32_for_crc32(days_since_epoch: i32) -> Result<String, String> {
    let days_from_ce = 719_163_i32
        .checked_add(days_since_epoch)
        .ok_or_else(|| format!("date32 day overflow: {days_since_epoch}"))?;
    let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce)
        .ok_or_else(|| format!("invalid date32 value: {days_since_epoch}"))?;
    Ok(date.format("%Y-%m-%d").to_string())
}

fn format_date64_for_crc32(millis_since_epoch: i64) -> Result<String, String> {
    let days = millis_since_epoch.div_euclid(86_400_000);
    let day_i32 = i32::try_from(days).map_err(|_| format!("date64 day overflow: {days}"))?;
    format_date32_for_crc32(day_i32)
}

fn format_timestamp_micros_for_crc32(micros_since_epoch: i64) -> Result<String, String> {
    let secs = micros_since_epoch.div_euclid(1_000_000);
    let micros = micros_since_epoch.rem_euclid(1_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, micros.saturating_mul(1_000))
        .ok_or_else(|| format!("invalid timestamp micros: {micros_since_epoch}"))?;
    let dt = dt.naive_utc();
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    if micros == 0 {
        Ok(base)
    } else {
        Ok(format!("{base}.{micros:06}"))
    }
}

fn crc32_hash_array_value(array: &dyn Array, row: usize, seed: u32) -> Result<u32, String> {
    if array.is_null(row) {
        return Ok(zlib_crc_hash(&0_i32.to_le_bytes(), seed));
    }
    match array.data_type() {
        arrow::datatypes::DataType::Int8 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast Int8Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::Int16 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast Int16Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::Int32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast Int32Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::Int64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast Int64Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::UInt8 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "downcast UInt8Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::UInt16 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "downcast UInt16Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::UInt32 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "downcast UInt32Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::UInt64 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "downcast UInt64Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::Float32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "downcast Float32Array failed".to_string())?;
            Ok(zlib_crc_hash(
                &typed.value(row).to_bits().to_le_bytes(),
                seed,
            ))
        }
        arrow::datatypes::DataType::Float64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "downcast Float64Array failed".to_string())?;
            Ok(zlib_crc_hash(
                &typed.value(row).to_bits().to_le_bytes(),
                seed,
            ))
        }
        arrow::datatypes::DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BooleanArray failed".to_string())?;
            let byte = if typed.value(row) { 1_u8 } else { 0_u8 };
            Ok(zlib_crc_hash(&[byte], seed))
        }
        arrow::datatypes::DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast StringArray failed".to_string())?;
            Ok(zlib_crc_hash(typed.value(row).as_bytes(), seed))
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "downcast LargeStringArray failed".to_string())?;
            Ok(zlib_crc_hash(typed.value(row).as_bytes(), seed))
        }
        arrow::datatypes::DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast BinaryArray failed".to_string())?;
            Ok(zlib_crc_hash(typed.value(row), seed))
        }
        arrow::datatypes::DataType::LargeBinary => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "downcast LargeBinaryArray failed".to_string())?;
            Ok(zlib_crc_hash(typed.value(row), seed))
        }
        arrow::datatypes::DataType::Decimal128(_, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast Decimal128Array failed".to_string())?;
            Ok(zlib_crc_hash(&typed.value(row).to_le_bytes(), seed))
        }
        arrow::datatypes::DataType::FixedSizeBinary(width) => {
            let typed = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "downcast FixedSizeBinaryArray failed".to_string())?;
            if typed.value_length() != *width {
                return Err(format!(
                    "fixed-size binary width mismatch: expected {}, actual {}",
                    width,
                    typed.value_length()
                ));
            }
            Ok(zlib_crc_hash(typed.value(row), seed))
        }
        arrow::datatypes::DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast Date32Array failed".to_string())?;
            let rendered = format_date32_for_crc32(typed.value(row))?;
            Ok(zlib_crc_hash(rendered.as_bytes(), seed))
        }
        arrow::datatypes::DataType::Date64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| "downcast Date64Array failed".to_string())?;
            let rendered = format_date64_for_crc32(typed.value(row))?;
            Ok(zlib_crc_hash(rendered.as_bytes(), seed))
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Second, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "downcast TimestampSecondArray failed".to_string())?;
            let micros = typed.value(row).saturating_mul(1_000_000);
            let rendered = format_timestamp_micros_for_crc32(micros)?;
            Ok(zlib_crc_hash(rendered.as_bytes(), seed))
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "downcast TimestampMillisecondArray failed".to_string())?;
            let micros = typed.value(row).saturating_mul(1_000);
            let rendered = format_timestamp_micros_for_crc32(micros)?;
            Ok(zlib_crc_hash(rendered.as_bytes(), seed))
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast TimestampMicrosecondArray failed".to_string())?;
            let rendered = format_timestamp_micros_for_crc32(typed.value(row))?;
            Ok(zlib_crc_hash(rendered.as_bytes(), seed))
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "downcast TimestampNanosecondArray failed".to_string())?;
            let micros = typed.value(row) / 1_000;
            let rendered = format_timestamp_micros_for_crc32(micros)?;
            Ok(zlib_crc_hash(rendered.as_bytes(), seed))
        }
        other => Err(format!("unsupported distributed hash type: {other:?}")),
    }
}

fn zlib_crc_hash(data: &[u8], seed: u32) -> u32 {
    let mut crc = seed ^ 0xffff_ffff;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xffff_ffff
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::sync::Arc;

    use arrow::array::{Date32Array, Int64Array, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use chrono::{Datelike, NaiveDate};

    use super::{
        RowRoutingPlan, build_output_expr_slot_name_map, build_sink_routing,
        crc32_hash_array_value, route_chunk_rows, zlib_crc_hash,
    };
    use crate::common::ids::SlotId;
    use crate::connector::starrocks::sink::partition_key::{PartitionKeySource, PartitionSlotRef};
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::{data_sinks, descriptors, exprs, types};

    fn build_test_schema() -> descriptors::TOlapTableSchemaParam {
        descriptors::TOlapTableSchemaParam {
            db_id: 1,
            table_id: 2,
            version: 1,
            slot_descs: vec![descriptors::TSlotDescriptor {
                id: Some(1),
                parent: None,
                slot_type: None,
                column_pos: None,
                byte_offset: None,
                null_indicator_byte: None,
                null_indicator_bit: None,
                col_name: Some("k".to_string()),
                slot_idx: None,
                is_materialized: None,
                is_output_column: None,
                is_nullable: Some(false),
                col_unique_id: None,
                col_physical_name: None,
            }],
            tuple_desc: descriptors::TTupleDescriptor {
                id: Some(1),
                byte_size: Some(8),
                num_null_bytes: Some(0),
                table_id: Some(2),
                num_null_slots: Some(0),
            },
            indexes: vec![descriptors::TOlapTableIndexSchema {
                id: 10,
                columns: vec!["k".to_string()],
                schema_hash: 1,
                column_param: None,
                where_clause: None,
                schema_id: Some(10),
                column_to_expr_value: None,
                is_shadow: None,
            }],
        }
    }

    fn build_test_sink(
        partitions: Vec<descriptors::TOlapTablePartition>,
        partition_columns: Option<Vec<String>>,
    ) -> data_sinks::TOlapTableSink {
        let mut tablet_ids = BTreeSet::new();
        for partition in &partitions {
            for index in &partition.indexes {
                for tablet_id in &index.tablet_ids {
                    tablet_ids.insert(*tablet_id);
                }
            }
        }
        let tablet_locations = tablet_ids
            .into_iter()
            .map(|tablet_id| descriptors::TTabletLocation {
                tablet_id,
                node_ids: vec![101],
            })
            .collect::<Vec<_>>();
        data_sinks::TOlapTableSink {
            load_id: types::TUniqueId { hi: 1, lo: 2 },
            txn_id: 10,
            db_id: 1,
            table_id: 2,
            tuple_id: 1,
            num_replicas: 1,
            need_gen_rollup: false,
            db_name: Some("db".to_string()),
            table_name: Some("tbl".to_string()),
            schema: build_test_schema(),
            partition: descriptors::TOlapTablePartitionParam {
                db_id: 1,
                table_id: 2,
                version: 1,
                partition_column: None,
                distributed_columns: Some(vec!["k".to_string()]),
                partitions,
                partition_columns,
                partition_exprs: None,
                enable_automatic_partition: Some(false),
            },
            location: descriptors::TOlapTableLocationParam {
                db_id: 1,
                table_id: 2,
                version: 1,
                tablets: tablet_locations,
            },
            nodes_info: descriptors::TNodesInfo {
                version: 1,
                nodes: vec![descriptors::TNodeInfo {
                    id: 101,
                    option: 0,
                    host: "127.0.0.1".to_string(),
                    async_internal_port: 1,
                }],
            },
            load_channel_timeout_s: None,
            is_lake_table: Some(true),
            txn_trace_parent: None,
            keys_type: Some(types::TKeysType::DUP_KEYS),
            write_quorum_type: None,
            enable_replicated_storage: None,
            merge_condition: None,
            null_expr_in_auto_increment: None,
            miss_auto_increment_column: None,
            abort_delete: None,
            auto_increment_slot_id: None,
            partial_update_mode: None,
            label: None,
            enable_colocate_mv_index: None,
            automatic_bucket_size: None,
            write_txn_log: None,
            ignore_out_of_partition: None,
            encryption_meta: None,
            dynamic_overwrite: None,
            enable_data_file_bundling: None,
            is_multi_statements_txn: None,
        }
    }

    fn build_test_chunk(values: Vec<i64>) -> Chunk {
        let field = field_with_slot_id(Field::new("k", DataType::Int64, false), SlotId::new(1));
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
            .expect("build test batch");
        Chunk::try_new(batch).expect("build chunk")
    }

    fn build_test_chunk_with_slot(slot_id: SlotId, values: Vec<i64>) -> Chunk {
        let field = field_with_slot_id(Field::new("k", DataType::Int64, false), slot_id);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
            .expect("build test batch");
        Chunk::try_new(batch).expect("build chunk")
    }

    fn create_dummy_type() -> types::TTypeDesc {
        types::TTypeDesc {
            types: Some(vec![types::TTypeNode {
                type_: types::TTypeNodeType::SCALAR,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn int_literal_node(value: i64) -> exprs::TExprNode {
        exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: create_dummy_type(),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: Some(exprs::TIntLiteral { value }),
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

    fn slot_ref_expr(slot_id: i32) -> exprs::TExpr {
        let mut node = int_literal_node(0);
        node.node_type = exprs::TExprNodeType::SLOT_REF;
        node.int_literal = None;
        node.slot_ref = Some(exprs::TSlotRef {
            slot_id,
            tuple_id: 0,
        });
        exprs::TExpr { nodes: vec![node] }
    }

    fn slot_desc(id: i32, col_name: Option<&str>) -> descriptors::TSlotDescriptor {
        descriptors::TSlotDescriptor {
            id: Some(id),
            parent: None,
            slot_type: None,
            column_pos: None,
            byte_offset: None,
            null_indicator_byte: None,
            null_indicator_bit: None,
            col_name: col_name.map(ToString::to_string),
            slot_idx: None,
            is_materialized: None,
            is_output_column: None,
            is_nullable: Some(true),
            col_unique_id: None,
            col_physical_name: None,
        }
    }

    fn build_range_partition(
        partition_id: i64,
        start_key: Option<i64>,
        end_key: i64,
        tablet_ids: Vec<i64>,
    ) -> descriptors::TOlapTablePartition {
        descriptors::TOlapTablePartition {
            id: partition_id,
            start_key: None,
            end_key: None,
            deprecated_num_buckets: None,
            indexes: vec![descriptors::TOlapTableIndexTablets {
                index_id: 10,
                tablet_ids,
                tablets: None,
            }],
            start_keys: start_key.map(|v| vec![int_literal_node(v)]),
            end_keys: Some(vec![int_literal_node(end_key)]),
            in_keys: None,
            is_shadow_partition: Some(false),
        }
    }

    fn map_rows_to_tablets(grouped: &[Vec<u32>], tablet_ids: &[i64]) -> HashMap<usize, i64> {
        let mut row_to_tablet = HashMap::new();
        for (tablet_idx, rows) in grouped.iter().enumerate() {
            let tablet_id = tablet_ids[tablet_idx];
            for row in rows {
                row_to_tablet.insert(*row as usize, tablet_id);
            }
        }
        row_to_tablet
    }

    fn route_rows(plan: &RowRoutingPlan, chunk: &Chunk) -> Result<Vec<Vec<u32>>, String> {
        let mut seed = 0;
        route_chunk_rows(plan, chunk, &mut seed)
    }

    #[test]
    fn output_expr_slot_map_skips_empty_slot_descriptors() {
        let mut sink = build_test_sink(Vec::new(), None);
        sink.schema.slot_descs = vec![
            slot_desc(1, None),
            slot_desc(2, Some("c0")),
            slot_desc(3, Some("c1")),
            slot_desc(4, Some("c2")),
        ];
        sink.schema.indexes[0].columns = vec!["c0".to_string(), "c1".to_string(), "c2".to_string()];
        sink.schema.indexes[0].column_param = None;

        let output_exprs = vec![slot_ref_expr(101), slot_ref_expr(102), slot_ref_expr(103)];
        let mapping = build_output_expr_slot_name_map(&sink, 10, Some(&output_exprs)).unwrap();

        assert_eq!(mapping.get("c0"), Some(&SlotId::new(101)));
        assert_eq!(mapping.get("c1"), Some(&SlotId::new(102)));
        assert_eq!(mapping.get("c2"), Some(&SlotId::new(103)));
    }

    #[test]
    fn output_expr_slot_map_skips_load_op_when_exprs_do_not_include_it() {
        let mut sink = build_test_sink(Vec::new(), None);
        sink.schema.slot_descs = vec![
            slot_desc(1, Some("__op")),
            slot_desc(2, Some("c0")),
            slot_desc(3, Some("c1")),
            slot_desc(4, Some("c2")),
        ];
        sink.schema.indexes[0].columns = vec!["c0".to_string(), "c1".to_string(), "c2".to_string()];
        sink.schema.indexes[0].column_param = None;

        let output_exprs = vec![slot_ref_expr(101), slot_ref_expr(102), slot_ref_expr(103)];
        let mapping = build_output_expr_slot_name_map(&sink, 10, Some(&output_exprs)).unwrap();

        assert_eq!(mapping.get("c0"), Some(&SlotId::new(101)));
        assert_eq!(mapping.get("c1"), Some(&SlotId::new(102)));
        assert_eq!(mapping.get("c2"), Some(&SlotId::new(103)));
    }

    #[test]
    fn same_key_rows_route_to_same_bucket() {
        let partition = descriptors::TOlapTablePartition {
            id: 11,
            start_key: None,
            end_key: None,
            deprecated_num_buckets: None,
            indexes: vec![descriptors::TOlapTableIndexTablets {
                index_id: 10,
                tablet_ids: vec![1001, 1002],
                tablets: None,
            }],
            start_keys: None,
            end_keys: None,
            in_keys: None,
            is_shadow_partition: Some(false),
        };
        let sink = build_test_sink(vec![partition], None);
        let routing = build_sink_routing(&sink, 10, None).expect("build sink routing");

        let chunk = build_test_chunk(vec![1, 2, 1, 2]);
        let grouped = route_rows(&routing.row_routing, &chunk).expect("route chunk rows by hash");

        let mut row_to_bucket = HashMap::new();
        for (bucket, rows) in grouped.iter().enumerate() {
            for row in rows {
                row_to_bucket.insert(*row as usize, bucket);
            }
        }
        assert_eq!(row_to_bucket.len(), 4);
        assert_eq!(row_to_bucket.get(&0), row_to_bucket.get(&2));
        assert_eq!(row_to_bucket.get(&1), row_to_bucket.get(&3));
    }

    #[test]
    fn range_multi_partition_multi_bucket_routing() {
        let part_a = build_range_partition(11, None, 10, vec![1001, 1002]);
        let part_b = build_range_partition(12, Some(10), 20, vec![1003, 1004]);
        let sink = build_test_sink(vec![part_a, part_b], Some(vec!["k".to_string()]));
        let routing = build_sink_routing(&sink, 10, None).expect("build sink routing");

        let chunk = build_test_chunk(vec![9, 9, 10, 10, 19, 19]);
        let grouped = route_rows(&routing.row_routing, &chunk)
            .expect("route rows for multi partition and multi bucket");
        let row_to_tablet = map_rows_to_tablets(&grouped, &routing.row_routing.tablet_ids);

        assert_eq!(row_to_tablet.len(), 6);
        assert_eq!(row_to_tablet.get(&0), row_to_tablet.get(&1));
        assert_eq!(row_to_tablet.get(&2), row_to_tablet.get(&3));
        assert_eq!(row_to_tablet.get(&4), row_to_tablet.get(&5));

        let part_a_tablets = HashSet::from([1001_i64, 1002_i64]);
        let part_b_tablets = HashSet::from([1003_i64, 1004_i64]);
        assert!(part_a_tablets.contains(row_to_tablet.get(&0).expect("row0")));
        assert!(part_b_tablets.contains(row_to_tablet.get(&2).expect("row2")));
        assert!(part_b_tablets.contains(row_to_tablet.get(&4).expect("row4")));
    }

    #[test]
    fn reject_row_not_in_any_partition() {
        let part_a = build_range_partition(11, None, 10, vec![1001, 1002]);
        let part_b = build_range_partition(12, Some(10), 20, vec![1003, 1004]);
        let sink = build_test_sink(vec![part_a, part_b], Some(vec!["k".to_string()]));
        let routing = build_sink_routing(&sink, 10, None).expect("build sink routing");

        let chunk = build_test_chunk(vec![21]);
        let err = route_rows(&routing.row_routing, &chunk)
            .expect_err("row outside partition range should fail");
        assert!(err.contains("does not match any partition"), "err={}", err);
    }

    #[test]
    fn crc32_hash_date32_uses_rendered_date_string() {
        let days = NaiveDate::from_ymd_opt(2024, 2, 29)
            .expect("valid date")
            .num_days_from_ce()
            - 719_163;
        let array = Date32Array::from(vec![days]);
        let actual =
            crc32_hash_array_value(&array, 0, 0).expect("hash date32 using rendered string bytes");
        let expected = zlib_crc_hash("2024-02-29".as_bytes(), 0);
        assert_eq!(actual, expected);
    }

    #[test]
    fn crc32_hash_timestamp_uses_rendered_datetime_string() {
        let micros = NaiveDate::from_ymd_opt(2024, 1, 2)
            .expect("valid date")
            .and_hms_micro_opt(3, 4, 5, 6_000)
            .expect("valid datetime")
            .and_utc()
            .timestamp_micros();
        let array = TimestampMicrosecondArray::from(vec![micros]);
        let actual =
            crc32_hash_array_value(&array, 0, 0).expect("hash timestamp using rendered string");
        let expected = zlib_crc_hash("2024-01-02 03:04:05.006000".as_bytes(), 0);
        assert_eq!(actual, expected);
    }

    #[test]
    fn partition_slot_ref_falls_back_to_column_name() {
        let partition = build_range_partition(11, None, 100, vec![1001, 1002]);
        let sink = build_test_sink(vec![partition], Some(vec!["k".to_string()]));
        let routing = build_sink_routing(&sink, 10, None).expect("build routing");
        let mut row_routing = routing.row_routing.clone();
        row_routing.partition_key_source = PartitionKeySource::SlotRefs(vec![PartitionSlotRef {
            slot_id: SlotId::new(0),
            column_name: "k".to_string(),
        }]);
        row_routing.partition_key_len = 1;

        let chunk = build_test_chunk(vec![42]);
        let grouped = route_rows(&row_routing, &chunk).expect("route with column-name fallback");
        let row_to_tablet = map_rows_to_tablets(&grouped, &row_routing.tablet_ids);
        assert_eq!(row_to_tablet.len(), 1);
    }

    #[test]
    fn output_expr_slot_map_overrides_schema_slot_ids() {
        let partition = build_range_partition(11, None, 100, vec![1001, 1002]);
        let sink = build_test_sink(vec![partition], Some(vec!["k".to_string()]));
        let output_exprs = vec![slot_ref_expr(5)];
        let routing = build_sink_routing(&sink, 10, Some(&output_exprs))
            .expect("build routing with output exprs");

        assert_eq!(
            routing.row_routing.distributed_slot_ids,
            vec![SlotId::new(5)]
        );
        match &routing.row_routing.partition_key_source {
            PartitionKeySource::SlotRefs(slot_refs) => {
                assert_eq!(slot_refs.len(), 1);
                assert_eq!(slot_refs[0].slot_id, SlotId::new(5));
            }
            other => panic!(
                "unexpected partition key source: {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    #[test]
    fn partition_expr_slot_refs_follow_output_expr_slot_ids() {
        let partition = build_range_partition(11, None, 100, vec![1001, 1002]);
        let mut sink = build_test_sink(vec![partition], None);
        sink.schema.slot_descs = vec![slot_desc(0, Some("k"))];
        sink.partition.partition_exprs = Some(vec![slot_ref_expr(0)]);

        let output_exprs = vec![slot_ref_expr(7)];
        let routing = build_sink_routing(&sink, 10, Some(&output_exprs))
            .expect("build routing with remapped partition expr slot ids");
        assert_eq!(
            routing.row_routing.distributed_slot_ids,
            vec![SlotId::new(7)]
        );

        let chunk = build_test_chunk_with_slot(SlotId::new(7), vec![42]);
        let grouped = route_rows(&routing.row_routing, &chunk)
            .expect("route rows with remapped partition expr slot ids");
        let row_to_tablet = map_rows_to_tablets(&grouped, &routing.row_routing.tablet_ids);
        assert_eq!(row_to_tablet.len(), 1);
    }

    #[test]
    fn falls_back_to_location_tablets_when_partitions_are_empty() {
        let mut sink = build_test_sink(Vec::new(), Some(vec!["k".to_string()]));
        sink.location.tablets = vec![
            descriptors::TTabletLocation {
                tablet_id: 2001,
                node_ids: vec![101],
            },
            descriptors::TTabletLocation {
                tablet_id: 2002,
                node_ids: vec![101],
            },
        ];

        let routing = build_sink_routing(&sink, 10, None)
            .expect("empty partition metadata should fallback to location tablets");
        assert_eq!(routing.row_routing.tablet_ids, vec![2001, 2002]);
        assert_eq!(routing.row_routing.partitions.len(), 1);
        assert_eq!(routing.row_routing.partitions[0].partition_id, 0);
        assert!(matches!(
            &routing.row_routing.partition_key_source,
            PartitionKeySource::None
        ));
        assert_eq!(routing.row_routing.partition_key_len, 0);
        assert_eq!(routing.tablet_to_partition.get(&2001), Some(&0));
        assert_eq!(routing.tablet_to_partition.get(&2002), Some(&0));

        let chunk = build_test_chunk(vec![1, 2, 3]);
        let grouped =
            route_rows(&routing.row_routing, &chunk).expect("route with location fallback");
        assert_eq!(grouped.iter().map(|rows| rows.len()).sum::<usize>(), 3);
    }

    #[test]
    fn falls_back_to_location_when_only_shadow_partitions_exist() {
        let shadow_partition = descriptors::TOlapTablePartition {
            id: 31,
            start_key: None,
            end_key: None,
            deprecated_num_buckets: None,
            indexes: vec![descriptors::TOlapTableIndexTablets {
                index_id: 10,
                tablet_ids: vec![3101],
                tablets: None,
            }],
            start_keys: None,
            end_keys: None,
            in_keys: None,
            is_shadow_partition: Some(true),
        };
        let sink = build_test_sink(vec![shadow_partition], None);
        let routing = build_sink_routing(&sink, 10, None)
            .expect("shadow-only partition metadata should fallback to location tablets");

        assert_eq!(routing.row_routing.partitions.len(), 1);
        assert_eq!(routing.row_routing.partitions[0].partition_id, 0);
        assert_eq!(routing.tablet_to_partition.get(&3101), Some(&0));
    }

    #[test]
    fn treats_partition_without_bounds_as_unpartitioned() {
        let partition = descriptors::TOlapTablePartition {
            id: 41,
            start_key: None,
            end_key: None,
            deprecated_num_buckets: None,
            indexes: vec![descriptors::TOlapTableIndexTablets {
                index_id: 10,
                tablet_ids: vec![4101, 4102],
                tablets: None,
            }],
            start_keys: None,
            end_keys: None,
            in_keys: None,
            is_shadow_partition: Some(false),
        };
        let sink = build_test_sink(vec![partition], Some(vec!["k".to_string()]));
        let routing = build_sink_routing(&sink, 10, None)
            .expect("partition without range/list bounds should route as unpartitioned");

        assert!(matches!(
            &routing.row_routing.partition_key_source,
            PartitionKeySource::None
        ));
        assert_eq!(routing.row_routing.partition_key_len, 0);
        let chunk = build_test_chunk(vec![1, 2, 3, 4]);
        let grouped = route_rows(&routing.row_routing, &chunk)
            .expect("route rows for unpartitioned fallback");
        assert_eq!(grouped.iter().map(|rows| rows.len()).sum::<usize>(), 4);
    }
}
