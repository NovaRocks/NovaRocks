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
use crate::connector::starrocks::sink::partition_key::{
    PartitionKeySource, PartitionKeyValue, PartitionMode, PartitionRoutingEntry,
    build_partition_key_arrays, build_partition_key_source, build_row_partition_key,
    compare_partition_key_vectors, partition_key_source_len, resolve_slot_ids_by_names,
    validate_partition_key_length,
};
use crate::connector::starrocks::sink::plan::{
    SinkLocationDescriptor, SinkPartitionDescriptor, StarRocksSinkDescriptor,
};
use crate::exec::chunk::Chunk;
use crate::runtime::sink_commit::TabletCommitInfo;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RowRejectReason {
    OutOfPartitionRanges,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RowRejection {
    pub(crate) row_index: u32,
    pub(crate) reason: RowRejectReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RoutedRows {
    pub(crate) per_tablet: Vec<Vec<u32>>,
    pub(crate) rejections: Vec<RowRejection>,
}

pub(crate) struct SinkRouting {
    pub(crate) commit_infos: Vec<TabletCommitInfo>,
    pub(crate) tablet_to_partition: BTreeMap<i64, i64>,
    pub(crate) row_routing: RowRoutingPlan,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn build_sink_routing(
    descriptor: &StarRocksSinkDescriptor,
    schema_id: i64,
    output_expr_slot_name_map: &HashMap<String, SlotId>,
) -> Result<SinkRouting, String> {
    let candidate_index_ids = descriptor
        .schema
        .indexes
        .iter()
        .filter(|idx| idx.schema_id == schema_id)
        .map(|idx| idx.index_id)
        .collect::<HashSet<_>>();
    if candidate_index_ids.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK cannot resolve routing index for schema_id={schema_id}"
        ));
    }
    build_sink_routing_with_candidates(
        descriptor,
        schema_id,
        candidate_index_ids,
        output_expr_slot_name_map,
    )
}

pub(crate) fn build_sink_routing_for_index_id(
    descriptor: &StarRocksSinkDescriptor,
    index_id: i64,
    schema_id: i64,
    output_expr_slot_name_map: &HashMap<String, SlotId>,
) -> Result<SinkRouting, String> {
    if index_id <= 0 {
        return Err(format!(
            "OLAP_TABLE_SINK cannot build routing for non-positive index_id={index_id}"
        ));
    }
    let mut candidate_index_ids = HashSet::new();
    candidate_index_ids.insert(index_id);
    build_sink_routing_with_candidates(
        descriptor,
        schema_id,
        candidate_index_ids,
        output_expr_slot_name_map,
    )
}

fn build_sink_routing_with_candidates(
    descriptor: &StarRocksSinkDescriptor,
    schema_id: i64,
    candidate_index_ids: HashSet<i64>,
    output_expr_slot_name_map: &HashMap<String, SlotId>,
) -> Result<SinkRouting, String> {
    let table_name = descriptor
        .table_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or("<unknown_table>");
    let db_name = descriptor
        .db_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or("<unknown_db>");

    let mut valid_backend_ids = HashSet::new();
    for node in &descriptor.nodes.nodes {
        valid_backend_ids.insert(node.id);
    }
    if valid_backend_ids.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK nodes_info is empty for {}.{} (table_id={})",
            db_name, table_name, descriptor.table_id
        ));
    }

    let mut tablet_to_backend = HashMap::new();
    for loc in &descriptor.location.tablets {
        let backend_id = *loc.node_ids.first().ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK location has empty node_ids for tablet {} on {}.{} (table_id={})",
                loc.tablet_id, db_name, table_name, descriptor.table_id
            )
        })?;
        if !valid_backend_ids.contains(&backend_id) {
            return Err(format!(
                "OLAP_TABLE_SINK location backend {} for tablet {} is not found in nodes_info on {}.{} (table_id={})",
                backend_id, loc.tablet_id, db_name, table_name, descriptor.table_id
            ));
        }
        if let Some(prev) = tablet_to_backend.insert(loc.tablet_id, backend_id)
            && prev != backend_id
        {
            return Err(format!(
                "OLAP_TABLE_SINK location backend mismatch for tablet {}: {} vs {} on {}.{} (table_id={})",
                loc.tablet_id, prev, backend_id, db_name, table_name, descriptor.table_id
            ));
        }
    }

    let partition_map = collect_tablet_partition_map(&descriptor.partition, &descriptor.location)?;
    let row_routing = build_row_routing_plan(
        descriptor,
        schema_id,
        &candidate_index_ids,
        output_expr_slot_name_map,
    )?;

    let mut commit_infos = Vec::with_capacity(row_routing.tablet_ids.len());
    for tablet_id in &row_routing.tablet_ids {
        let backend_id = *tablet_to_backend.get(tablet_id).ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK location missing mapping for tablet {} on {}.{} (table_id={})",
                tablet_id, db_name, table_name, descriptor.table_id
            )
        })?;
        commit_infos.push(TabletCommitInfo {
            tablet_id: *tablet_id,
            backend_id,
        });
        if !partition_map.contains_key(tablet_id) {
            return Err(format!(
                "missing partition id for tablet {} while building sink refs",
                tablet_id
            ));
        }
    }

    Ok(SinkRouting {
        commit_infos,
        tablet_to_partition: partition_map,
        row_routing,
    })
}

fn build_row_routing_plan(
    descriptor: &StarRocksSinkDescriptor,
    schema_id: i64,
    candidate_index_ids: &HashSet<i64>,
    output_expr_slot_name_map: &HashMap<String, SlotId>,
) -> Result<RowRoutingPlan, String> {
    let visible_partitions = descriptor
        .partition
        .partitions
        .iter()
        .filter(|part| !part.is_shadow)
        .collect::<Vec<_>>();

    if candidate_index_ids.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK cannot resolve routing index for schema_id={schema_id}"
        ));
    }

    let slot_name_overrides = if output_expr_slot_name_map.is_empty() {
        None
    } else {
        Some(output_expr_slot_name_map)
    };
    let distributed_slot_ids = resolve_distributed_slot_ids(
        &descriptor.schema,
        &descriptor.partition,
        slot_name_overrides,
    )?;
    if visible_partitions.is_empty() {
        return build_location_only_row_routing(&descriptor.location, distributed_slot_ids);
    }
    let routing_partitions = visible_partitions;

    let mut partition_key_source = build_partition_key_source(
        &descriptor.partition,
        &descriptor.schema,
        slot_name_overrides,
    )?;
    let mut partition_key_len = partition_key_source_len(&partition_key_source);

    let has_any_in_keys = routing_partitions
        .iter()
        .any(|part| !part.in_keys.is_empty());
    let has_any_range_bound = routing_partitions
        .iter()
        .any(|part| part.start_key.is_some() || part.end_key.is_some());
    let partition_mode = if partition_key_len == 0 || (!has_any_in_keys && !has_any_range_bound) {
        PartitionMode::Unpartitioned
    } else if has_any_in_keys {
        if routing_partitions
            .iter()
            .any(|part| part.in_keys.is_empty())
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
                    partition.partition_id, schema_id, candidate_index_ids
                )
            })?;
        if index.tablet_ids.is_empty() {
            return Err(format!(
                "OLAP_TABLE_SINK partition {} index {} has empty tablet_ids",
                partition.partition_id, index.index_id
            ));
        }

        let start_key = partition.start_key.clone();
        let end_key = partition.end_key.clone();
        let in_keys = partition.in_keys.clone();

        validate_partition_key_length(
            partition.partition_id,
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
                        partition.partition_id
                    ));
                }
            }
            PartitionMode::List => {
                if in_keys.is_empty() {
                    return Err(format!(
                        "OLAP_TABLE_SINK list partition {} has empty in_keys",
                        partition.partition_id
                    ));
                }
            }
        }

        for tablet_id in &index.tablet_ids {
            tablet_ids.insert(*tablet_id);
        }
        partitions.push(PartitionRoutingEntry {
            partition_id: partition.partition_id,
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
    location: &SinkLocationDescriptor,
    distributed_slot_ids: Vec<SlotId>,
) -> Result<RowRoutingPlan, String> {
    let mut tablet_ids = BTreeSet::new();
    for loc in &location.tablets {
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
    partition: &SinkPartitionDescriptor,
    location: &SinkLocationDescriptor,
) -> Result<BTreeMap<i64, i64>, String> {
    let mut visible_map = BTreeMap::new();
    for part in &partition.partitions {
        if part.is_shadow {
            continue;
        }
        for index in &part.indexes {
            for tablet_id in &index.tablet_ids {
                if let Some(existing) = visible_map.insert(*tablet_id, part.partition_id)
                    && existing != part.partition_id
                {
                    return Err(format!(
                        "tablet {} appears in multiple partitions: {} vs {}",
                        tablet_id, existing, part.partition_id
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
    schema: &crate::connector::starrocks::sink::plan::SinkSchemaDescriptor,
    partition: &SinkPartitionDescriptor,
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
) -> Result<Vec<SlotId>, String> {
    if partition.distributed_columns.is_empty() {
        return Ok(Vec::new());
    }
    resolve_slot_ids_by_names(
        &schema.slot_descs,
        &partition.distributed_columns,
        "distributed columns",
        slot_name_overrides,
    )
}

pub(crate) fn route_chunk_rows(
    row_routing: &RowRoutingPlan,
    chunk: &Chunk,
    next_random_hash: &mut u32,
) -> Result<RoutedRows, String> {
    if row_routing.tablet_ids.is_empty() {
        return Err("OLAP_TABLE_SINK has empty tablet routing".to_string());
    }
    if row_routing.partitions.is_empty() {
        return Err("OLAP_TABLE_SINK has empty partition routing".to_string());
    }

    let mut per_tablet = vec![Vec::<u32>::new(); row_routing.tablet_ids.len()];
    let mut rejections = Vec::new();
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

        let Some(partition) = select_partition_for_row(row_routing, &row_key, hash)? else {
            rejections.push(RowRejection {
                row_index: u32::try_from(row).map_err(|_| format!("row index overflow: {row}"))?,
                reason: RowRejectReason::OutOfPartitionRanges,
            });
            continue;
        };
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

    Ok(RoutedRows {
        per_tablet,
        rejections,
    })
}

fn select_partition_for_row<'a>(
    row_routing: &'a RowRoutingPlan,
    row_key: &[PartitionKeyValue],
    hash: u32,
) -> Result<Option<&'a PartitionRoutingEntry>, String> {
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
        return Ok(None);
    }
    let selected_idx = candidates[(hash as usize) % candidates.len()];
    row_routing
        .partitions
        .get(selected_idx)
        .map(Some)
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
