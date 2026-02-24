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

use crate::connector::starrocks::lake::context::{TabletWriteContext, register_tablet_runtime};
use crate::formats::starrocks::writer::bundle_meta::{
    empty_tablet_metadata, load_latest_tablet_metadata, load_tablet_metadata_at_version,
    write_bundle_meta_file,
};
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::service::grpc_client::proto::starrocks::{
    ColumnPb, CompactionStrategyPb, CompressionTypePb, FlatJsonConfigPb, KeysType,
    PersistentIndexTypePb, TabletSchemaPb,
};
pub(crate) fn build_sink_tablet_schema(
    schema: &crate::descriptors::TOlapTableSchemaParam,
    schema_id: i64,
    keys_type: KeysType,
) -> Result<TabletSchemaPb, String> {
    if schema.slot_descs.is_empty() {
        return Err("OLAP_TABLE_SINK schema.slot_descs is empty".to_string());
    }
    let index = schema
        .indexes
        .iter()
        .find(|idx| {
            let effective_schema_id = idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id);
            effective_schema_id == schema_id
        })
        .ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK cannot find schema index by schema_id={schema_id} in schema.indexes"
            )
        })?;
    let column_param = index.column_param.as_ref().ok_or_else(|| {
        format!("OLAP_TABLE_SINK schema.indexes(schema_id={schema_id}) missing column_param")
    })?;
    if column_param.columns.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK schema.indexes(schema_id={schema_id}) has empty column_param.columns"
        ));
    }
    let slot_descs_by_name = build_slot_descs_by_name(schema)?;

    let mut columns = Vec::with_capacity(column_param.columns.len());
    let mut max_unique_id = 0i32;
    let mut used_unique_ids = HashSet::new();
    let mut unique_id_to_index = HashMap::new();

    for (idx, col) in column_param.columns.iter().enumerate() {
        let name = col.column_name.trim().to_string();
        if name.is_empty() {
            return Err(format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] has empty column_name",
                idx
            ));
        }
        let mut column_pb =
            resolve_sink_column_pb(col, &name, idx, schema_id, &slot_descs_by_name)?;

        let unique_id = col.col_unique_id.ok_or_else(|| {
            format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing col_unique_id",
                idx
            )
        })?;
        if unique_id < 0 {
            return Err(format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] has negative col_unique_id={}",
                idx, unique_id
            ));
        }
        if used_unique_ids.contains(&unique_id) {
            return Err(format!(
                "duplicate col_unique_id detected in schema.indexes(schema_id={}): unique_id={}",
                schema_id, unique_id
            ));
        }
        let is_key = col.is_key.ok_or_else(|| {
            format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing is_key",
                idx
            )
        })?;
        let is_nullable = col.is_allow_null.ok_or_else(|| {
            format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing is_allow_null",
                idx
            )
        })?;
        let aggregation =
            map_aggregation_type_to_schema_string(col.aggregation_type, is_key, keys_type, idx)?;
        if let Some(index_len) = col.index_len {
            column_pb.index_length = Some(index_len);
        }
        normalize_column_pb_type_attrs(&mut column_pb);
        if column_pb.r#type == "VARCHAR" && column_pb.index_length.is_none() {
            column_pb.index_length = Some(10);
        }

        used_unique_ids.insert(unique_id);
        unique_id_to_index.insert(unique_id, idx);
        max_unique_id = max_unique_id.max(unique_id);

        column_pb.unique_id = unique_id;
        column_pb.name = Some(name);
        column_pb.is_key = Some(is_key);
        column_pb.aggregation = aggregation;
        column_pb.is_nullable = Some(is_nullable);
        column_pb.default_value = col.default_value.as_ref().map(|v| v.as_bytes().to_vec());
        column_pb.is_bf_column = col.is_bloom_filter_column;
        column_pb.has_bitmap_index = col.has_bitmap_index;
        column_pb.is_auto_increment = Some(col.is_auto_increment.unwrap_or(false));

        columns.push(column_pb);
    }

    if column_param.short_key_column_count < 0 {
        return Err(format!(
            "schema.indexes(schema_id={schema_id}).column_param.short_key_column_count is negative: {}",
            column_param.short_key_column_count
        ));
    }
    let num_short_key_columns = column_param.short_key_column_count;
    if num_short_key_columns as usize > columns.len() {
        return Err(format!(
            "short_key_column_count exceeds column count: short_key_column_count={} columns={}",
            num_short_key_columns,
            columns.len()
        ));
    }
    let mut sort_key_unique_ids = Vec::new();
    let mut sort_key_idxes = Vec::new();
    if !column_param.sort_key_uid.is_empty() {
        sort_key_unique_ids.reserve(column_param.sort_key_uid.len());
        sort_key_idxes.reserve(column_param.sort_key_uid.len());
        for (idx, unique_id) in column_param.sort_key_uid.iter().enumerate() {
            if *unique_id < 0 {
                return Err(format!(
                    "schema.indexes(schema_id={schema_id}).column_param.sort_key_uid[{}] is negative: {}",
                    idx, unique_id
                ));
            }
            let col_idx = unique_id_to_index.get(unique_id).ok_or_else(|| {
                format!(
                    "schema.indexes(schema_id={schema_id}).column_param.sort_key_uid[{}]={} not found in columns",
                    idx, unique_id
                )
            })?;
            sort_key_unique_ids.push(*unique_id as u32);
            sort_key_idxes.push(*col_idx as u32);
        }
    } else {
        for (idx, col) in columns.iter().enumerate() {
            if col.is_key.unwrap_or(false) {
                sort_key_unique_ids.push(col.unique_id as u32);
                sort_key_idxes.push(idx as u32);
            }
        }
    }
    if sort_key_idxes.is_empty() {
        return Err(format!(
            "schema.indexes(schema_id={schema_id}) resolved empty sort key columns"
        ));
    }

    Ok(TabletSchemaPb {
        keys_type: Some(keys_type as i32),
        column: columns.clone(),
        num_short_key_columns: Some(num_short_key_columns),
        num_rows_per_row_block: None,
        bf_fpp: None,
        next_column_unique_id: Some((max_unique_id + 1) as u32),
        deprecated_is_in_memory: None,
        deprecated_id: None,
        compression_type: None,
        sort_key_idxes,
        schema_version: Some(0),
        sort_key_unique_ids,
        table_indices: Vec::new(),
        compression_level: None,
        id: Some(schema_id),
    })
}

pub(crate) fn create_lake_tablet_from_req(
    request: &crate::agent_service::TCreateTabletReq,
    tablet_root_path: &str,
    s3_config: Option<S3StoreConfig>,
) -> Result<(), String> {
    let tablet_id = request.tablet_id;
    if tablet_id <= 0 {
        return Err(format!(
            "create_tablet has non-positive tablet_id={tablet_id}"
        ));
    }

    let tablet_schema = build_create_tablet_schema(request)?;
    let runtime_ctx = TabletWriteContext {
        db_id: 0,
        table_id: request.table_id.unwrap_or(0),
        tablet_id,
        tablet_root_path: tablet_root_path.to_string(),
        tablet_schema: tablet_schema.clone(),
        s3_config,
        partial_update: Default::default(),
    };
    register_tablet_runtime(&runtime_ctx)?;

    let existing_base = match load_tablet_metadata_at_version(tablet_root_path, tablet_id, 1) {
        Ok(v) => v,
        Err(err) if is_missing_tablet_page_in_bundle_error(&err) => None,
        Err(err) => return Err(err),
    };
    if existing_base.is_some() {
        return Ok(());
    }

    let (latest_version, _) = load_latest_tablet_metadata(tablet_root_path, tablet_id)?;
    if latest_version > 1 {
        return Ok(());
    }

    let persistent_index_type = match request.persistent_index_type {
        Some(v) => Some(map_create_tablet_persistent_index_type(v)? as i32),
        None => None,
    };
    let compaction_strategy = request
        .compaction_strategy
        .map(map_create_tablet_compaction_strategy)
        .transpose()?
        .or(Some(CompactionStrategyPb::Default as i32));
    let flat_json_config = request
        .flat_json_config
        .as_ref()
        .map(|cfg| FlatJsonConfigPb {
            flat_json_enable: cfg.flat_json_enable,
            flat_json_null_factor: cfg.flat_json_null_factor.map(|v| v.0),
            flat_json_sparsity_factor: cfg.flat_json_sparsity_factor.map(|v| v.0),
            flat_json_max_column_max: cfg.flat_json_column_max,
        });

    let mut tablet_meta = empty_tablet_metadata(tablet_id);
    tablet_meta.version = Some(1);
    tablet_meta.enable_persistent_index = request.enable_persistent_index;
    tablet_meta.persistent_index_type = persistent_index_type;
    tablet_meta.gtid = Some(request.gtid.unwrap_or(0));
    tablet_meta.compaction_strategy = compaction_strategy;
    tablet_meta.flat_json_config = flat_json_config;
    write_bundle_meta_file(tablet_root_path, tablet_id, 1, &tablet_schema, &tablet_meta)
}

fn is_missing_tablet_page_in_bundle_error(error: &str) -> bool {
    error.contains("bundle metadata missing tablet page for tablet_id=")
}

fn build_create_tablet_schema(
    request: &crate::agent_service::TCreateTabletReq,
) -> Result<TabletSchemaPb, String> {
    let schema = &request.tablet_schema;
    if schema.columns.is_empty() {
        return Err(format!(
            "create_tablet tablet_schema.columns is empty for tablet_id={}",
            request.tablet_id
        ));
    }

    let keys_type = map_create_tablet_keys_type(schema.keys_type)?;
    let mut columns = Vec::with_capacity(schema.columns.len());
    let mut max_unique_id = 0_i32;
    let mut used_unique_ids = HashSet::with_capacity(schema.columns.len());
    let mut unique_id_to_index = HashMap::with_capacity(schema.columns.len());

    for (idx, col) in schema.columns.iter().enumerate() {
        let name = col.column_name.trim().to_string();
        if name.is_empty() {
            return Err(format!(
                "create_tablet tablet_schema.columns[{}] has empty column_name",
                idx
            ));
        }

        let mut column_pb = resolve_create_tablet_column_pb(col, idx)?;

        let unique_id = col.col_unique_id.unwrap_or(idx as i32);
        let effective_unique_id = if unique_id < 0 { idx as i32 } else { unique_id };
        if used_unique_ids.contains(&effective_unique_id) {
            return Err(format!(
                "create_tablet has duplicate col_unique_id={}",
                effective_unique_id
            ));
        }
        used_unique_ids.insert(effective_unique_id);
        unique_id_to_index.insert(effective_unique_id, idx);
        max_unique_id = max_unique_id.max(effective_unique_id);

        let is_key = col.is_key.unwrap_or(false);
        let aggregation =
            map_aggregation_type_to_schema_string(col.aggregation_type, is_key, keys_type, idx)?;
        if let Some(index_len) = col.index_len {
            column_pb.index_length = Some(index_len);
        }
        normalize_column_pb_type_attrs(&mut column_pb);
        if column_pb.r#type == "VARCHAR" && column_pb.index_length.is_none() {
            column_pb.index_length = Some(10);
        }

        column_pb.unique_id = effective_unique_id;
        column_pb.name = Some(name);
        column_pb.is_key = Some(is_key);
        column_pb.aggregation = aggregation;
        column_pb.is_nullable = Some(col.is_allow_null.unwrap_or(false));
        column_pb.default_value = col.default_value.as_ref().map(|v| v.as_bytes().to_vec());
        column_pb.is_bf_column = col.is_bloom_filter_column;
        column_pb.has_bitmap_index = col.has_bitmap_index;
        column_pb.is_auto_increment = Some(col.is_auto_increment.unwrap_or(false));

        columns.push(column_pb);
    }

    let num_short_key_columns = i32::from(schema.short_key_column_count);
    if num_short_key_columns < 0 {
        return Err(format!(
            "create_tablet tablet_schema.short_key_column_count is negative: {}",
            num_short_key_columns
        ));
    }
    if num_short_key_columns as usize > columns.len() {
        return Err(format!(
            "create_tablet short_key_column_count exceeds column count: short_key_column_count={} columns={}",
            num_short_key_columns,
            columns.len()
        ));
    }

    let mut sort_key_idxes = Vec::new();
    if let Some(raw_sort_key_idxes) = schema.sort_key_idxes.as_ref() {
        sort_key_idxes.reserve(raw_sort_key_idxes.len());
        for (idx, value) in raw_sort_key_idxes.iter().enumerate() {
            if *value < 0 || (*value as usize) >= columns.len() {
                return Err(format!(
                    "create_tablet tablet_schema.sort_key_idxes[{}] is out of range: {}",
                    idx, value
                ));
            }
            sort_key_idxes.push(*value as u32);
        }
    }

    let mut sort_key_unique_ids = Vec::new();
    if let Some(raw_sort_key_unique_ids) = schema.sort_key_unique_ids.as_ref() {
        sort_key_unique_ids.reserve(raw_sort_key_unique_ids.len());
        for (idx, unique_id) in raw_sort_key_unique_ids.iter().enumerate() {
            if *unique_id < 0 {
                return Err(format!(
                    "create_tablet tablet_schema.sort_key_unique_ids[{}] is negative: {}",
                    idx, unique_id
                ));
            }
            if !unique_id_to_index.contains_key(unique_id) {
                return Err(format!(
                    "create_tablet tablet_schema.sort_key_unique_ids[{}]={} not found in columns",
                    idx, unique_id
                ));
            }
            sort_key_unique_ids.push(*unique_id as u32);
        }
    }

    if sort_key_idxes.is_empty() && sort_key_unique_ids.is_empty() {
        for (idx, col) in columns.iter().enumerate() {
            if col.is_key == Some(true) {
                sort_key_idxes.push(idx as u32);
                sort_key_unique_ids.push(col.unique_id as u32);
            }
        }
    }

    let fallback_next_unique_id = columns.len() as u32;
    let next_column_unique_id = max_unique_id
        .saturating_add(1)
        .max(fallback_next_unique_id as i32) as u32;
    let compression = request
        .compression_type
        .or(schema.compression_type)
        .unwrap_or(crate::types::TCompressionType::LZ4_FRAME);
    let compression_type = map_create_tablet_compression_type(compression)? as i32;
    let compression_level = request
        .compression_level
        .or(schema.compression_level)
        .or(Some(-1));

    Ok(TabletSchemaPb {
        keys_type: Some(keys_type as i32),
        column: columns,
        num_short_key_columns: Some(num_short_key_columns),
        num_rows_per_row_block: None,
        bf_fpp: schema.bloom_filter_fpp.map(|v| v.0),
        next_column_unique_id: Some(next_column_unique_id),
        deprecated_is_in_memory: schema.is_in_memory,
        deprecated_id: None,
        compression_type: Some(compression_type),
        sort_key_idxes,
        schema_version: schema.schema_version,
        sort_key_unique_ids,
        table_indices: Vec::new(),
        compression_level,
        id: schema.id,
    })
}

fn resolve_create_tablet_column_pb(
    column: &crate::descriptors::TColumn,
    column_idx: usize,
) -> Result<ColumnPb, String> {
    if let Some(type_desc) = column.type_desc.as_ref() {
        return build_create_tablet_column_pb_from_type_desc(type_desc, column_idx);
    }
    if let Some(column_type) = column.column_type.as_ref() {
        return build_create_tablet_column_pb_from_column_type(column_type, column_idx);
    }
    Err(format!(
        "create_tablet column {} missing both column_type and type_desc",
        column_idx
    ))
}

fn build_create_tablet_column_pb_from_column_type(
    column_type: &crate::types::TColumnType,
    column_idx: usize,
) -> Result<ColumnPb, String> {
    let sr_type = map_primitive_to_starrocks_type(column_type.type_).ok_or_else(|| {
        format!(
            "create_tablet has unsupported primitive type {:?} in column {}",
            column_type.type_, column_idx
        )
    })?;
    Ok(ColumnPb {
        unique_id: -1,
        name: None,
        r#type: sr_type.to_string(),
        is_key: Some(false),
        aggregation: Some("NONE".to_string()),
        is_nullable: Some(true),
        default_value: None,
        precision: column_type.precision,
        frac: column_type.scale,
        length: column_type.len,
        index_length: column_type.index_len.or(column_type.len),
        is_bf_column: None,
        referenced_column_id: None,
        referenced_column: None,
        has_bitmap_index: None,
        visible: None,
        children_columns: Vec::new(),
        is_auto_increment: Some(false),
        agg_state_desc: None,
    })
}

fn build_create_tablet_column_pb_from_type_desc(
    type_desc: &crate::types::TTypeDesc,
    column_idx: usize,
) -> Result<ColumnPb, String> {
    let nodes = type_desc.types.as_ref().ok_or_else(|| {
        format!(
            "create_tablet column {} has empty type_desc.types",
            column_idx
        )
    })?;
    if nodes.is_empty() {
        return Err(format!(
            "create_tablet column {} has empty type_desc.types",
            column_idx
        ));
    }
    let mut cursor = 0usize;
    let mut column_pb = init_create_tablet_sub_field_pb();
    type_desc_to_column_pb(nodes, &mut cursor, column_idx, "root", &mut column_pb)?;
    if cursor != nodes.len() {
        return Err(format!(
            "create_tablet column {} type_desc parse did not consume all nodes: consumed={} total={}",
            column_idx,
            cursor,
            nodes.len()
        ));
    }
    Ok(column_pb)
}

fn type_desc_to_column_pb(
    nodes: &[crate::types::TTypeNode],
    cursor: &mut usize,
    column_idx: usize,
    path: &str,
    column_pb: &mut ColumnPb,
) -> Result<(), String> {
    let node = nodes.get(*cursor).ok_or_else(|| {
        format!(
            "create_tablet column {} type_desc parse out of bounds at path={} cursor={} total_nodes={}",
            column_idx,
            path,
            *cursor,
            nodes.len()
        )
    })?;
    *cursor += 1;

    if node.type_ == crate::types::TTypeNodeType::SCALAR {
        let scalar = node.scalar_type.as_ref().ok_or_else(|| {
            format!(
                "create_tablet column {} scalar node missing scalar_type at path={}",
                column_idx, path
            )
        })?;
        let sr_type = map_primitive_to_starrocks_type(scalar.type_).ok_or_else(|| {
            format!(
                "create_tablet column {} has unsupported primitive type {:?} at path={}",
                column_idx, scalar.type_, path
            )
        })?;
        column_pb.r#type = sr_type.to_string();
        column_pb.precision = scalar.precision;
        column_pb.frac = scalar.scale;
        column_pb.length = scalar.len;
        column_pb.index_length = scalar.len;
        return Ok(());
    }

    if node.type_ == crate::types::TTypeNodeType::ARRAY {
        column_pb.r#type = "ARRAY".to_string();
        let mut element = init_create_tablet_sub_field_pb();
        type_desc_to_column_pb(
            nodes,
            cursor,
            column_idx,
            &format!("{path}.element"),
            &mut element,
        )?;
        element.name = Some("element".to_string());
        column_pb.children_columns.push(element);
        return Ok(());
    }

    if node.type_ == crate::types::TTypeNodeType::MAP {
        column_pb.r#type = "MAP".to_string();
        let mut key = init_create_tablet_sub_field_pb();
        type_desc_to_column_pb(nodes, cursor, column_idx, &format!("{path}.key"), &mut key)?;
        key.name = Some("key".to_string());
        column_pb.children_columns.push(key);

        let mut value = init_create_tablet_sub_field_pb();
        type_desc_to_column_pb(
            nodes,
            cursor,
            column_idx,
            &format!("{path}.value"),
            &mut value,
        )?;
        value.name = Some("value".to_string());
        column_pb.children_columns.push(value);
        return Ok(());
    }

    if node.type_ == crate::types::TTypeNodeType::STRUCT {
        column_pb.r#type = "STRUCT".to_string();
        let struct_fields = node.struct_fields.as_ref().ok_or_else(|| {
            format!(
                "create_tablet column {} struct node missing struct_fields at path={}",
                column_idx, path
            )
        })?;
        for (idx, field) in struct_fields.iter().enumerate() {
            let field_name = field
                .name
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    format!(
                        "create_tablet column {} struct field {} has empty name at path={}",
                        column_idx, idx, path
                    )
                })?;
            let mut field_pb = init_create_tablet_sub_field_pb();
            type_desc_to_column_pb(
                nodes,
                cursor,
                column_idx,
                &format!("{path}.{field_name}"),
                &mut field_pb,
            )?;
            field_pb.name = Some(field_name.to_string());
            if let Some(field_id) = field.id
                && field_id >= 0
            {
                field_pb.unique_id = field_id;
            }
            column_pb.children_columns.push(field_pb);
        }
        return Ok(());
    }

    Err(format!(
        "create_tablet column {} has unsupported type_desc node {:?} at path={}",
        column_idx, node.type_, path
    ))
}

fn init_create_tablet_sub_field_pb() -> ColumnPb {
    ColumnPb {
        unique_id: -1,
        name: None,
        r#type: String::new(),
        is_key: Some(false),
        aggregation: Some("NONE".to_string()),
        is_nullable: Some(true),
        default_value: None,
        precision: None,
        frac: None,
        length: None,
        index_length: None,
        is_bf_column: None,
        referenced_column_id: None,
        referenced_column: None,
        has_bitmap_index: None,
        visible: None,
        children_columns: Vec::new(),
        is_auto_increment: Some(false),
        agg_state_desc: None,
    }
}

fn normalize_column_pb_type_attrs(column: &mut ColumnPb) {
    if column.length.is_some_and(|v| v < 0) {
        column.length = None;
    }
    if column.index_length.is_some_and(|v| v < 0) {
        column.index_length = None;
    }
    if column.precision.is_some_and(|v| v < 0) {
        column.precision = None;
    }
    if column.frac.is_some_and(|v| v < 0) {
        column.frac = None;
    }
    for child in column.children_columns.iter_mut() {
        normalize_column_pb_type_attrs(child);
    }
}

fn map_create_tablet_keys_type(keys_type: crate::types::TKeysType) -> Result<KeysType, String> {
    if keys_type == crate::types::TKeysType::DUP_KEYS {
        return Ok(KeysType::DupKeys);
    }
    if keys_type == crate::types::TKeysType::UNIQUE_KEYS {
        return Ok(KeysType::UniqueKeys);
    }
    if keys_type == crate::types::TKeysType::AGG_KEYS {
        return Ok(KeysType::AggKeys);
    }
    if keys_type == crate::types::TKeysType::PRIMARY_KEYS {
        return Ok(KeysType::PrimaryKeys);
    }
    Err(format!(
        "unsupported create_tablet keys_type={:?}",
        keys_type
    ))
}

fn map_create_tablet_compression_type(
    compression_type: crate::types::TCompressionType,
) -> Result<CompressionTypePb, String> {
    if compression_type == crate::types::TCompressionType::DEFAULT_COMPRESSION {
        return Ok(CompressionTypePb::DefaultCompression);
    }
    if compression_type == crate::types::TCompressionType::NO_COMPRESSION {
        return Ok(CompressionTypePb::NoCompression);
    }
    if compression_type == crate::types::TCompressionType::SNAPPY {
        return Ok(CompressionTypePb::Snappy);
    }
    if compression_type == crate::types::TCompressionType::LZ4
        || compression_type == crate::types::TCompressionType::LZ4_FRAME
    {
        return Ok(CompressionTypePb::Lz4Frame);
    }
    if compression_type == crate::types::TCompressionType::ZLIB {
        return Ok(CompressionTypePb::Zlib);
    }
    if compression_type == crate::types::TCompressionType::ZSTD {
        return Ok(CompressionTypePb::Zstd);
    }
    if compression_type == crate::types::TCompressionType::GZIP {
        return Ok(CompressionTypePb::Gzip);
    }
    if compression_type == crate::types::TCompressionType::DEFLATE {
        return Ok(CompressionTypePb::Deflate);
    }
    if compression_type == crate::types::TCompressionType::BZIP2 {
        return Ok(CompressionTypePb::Bzip2);
    }
    if compression_type == crate::types::TCompressionType::BROTLI {
        return Ok(CompressionTypePb::Brotli);
    }
    Err(format!(
        "unsupported create_tablet compression_type={:?}",
        compression_type
    ))
}

fn map_create_tablet_persistent_index_type(
    persistent_index_type: crate::agent_service::TPersistentIndexType,
) -> Result<PersistentIndexTypePb, String> {
    if persistent_index_type == crate::agent_service::TPersistentIndexType::LOCAL {
        return Ok(PersistentIndexTypePb::Local);
    }
    if persistent_index_type == crate::agent_service::TPersistentIndexType::CLOUD_NATIVE {
        return Ok(PersistentIndexTypePb::CloudNative);
    }
    Err(format!(
        "unsupported create_tablet persistent_index_type={:?}",
        persistent_index_type
    ))
}

fn map_create_tablet_compaction_strategy(
    compaction_strategy: crate::agent_service::TCompactionStrategy,
) -> Result<i32, String> {
    if compaction_strategy == crate::agent_service::TCompactionStrategy::DEFAULT {
        return Ok(CompactionStrategyPb::Default as i32);
    }
    if compaction_strategy == crate::agent_service::TCompactionStrategy::REAL_TIME {
        return Ok(CompactionStrategyPb::RealTime as i32);
    }
    Err(format!(
        "unsupported create_tablet compaction_strategy={:?}",
        compaction_strategy
    ))
}

fn build_slot_descs_by_name(
    schema: &crate::descriptors::TOlapTableSchemaParam,
) -> Result<HashMap<String, &crate::descriptors::TSlotDescriptor>, String> {
    let mut map = HashMap::new();
    for (idx, slot) in schema.slot_descs.iter().enumerate() {
        let name = slot
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("schema.slot_descs[{}] missing col_name", idx))?
            .to_ascii_lowercase();
        if map.insert(name.clone(), slot).is_some() {
            return Err(format!(
                "duplicate col_name in schema.slot_descs is not supported: {}",
                name
            ));
        }
    }
    Ok(map)
}

fn resolve_sink_column_pb(
    column: &crate::descriptors::TColumn,
    column_name: &str,
    column_idx: usize,
    schema_id: i64,
    slot_descs_by_name: &HashMap<String, &crate::descriptors::TSlotDescriptor>,
) -> Result<ColumnPb, String> {
    if let Some(column_type) = column.column_type.as_ref() {
        return build_create_tablet_column_pb_from_column_type(column_type, column_idx).map_err(
            |err| {
                format!(
                    "schema.indexes(schema_id={schema_id}).column_param.columns[{}] has unsupported column_type (col_name={}): {}",
                    column_idx, column_name, err
                )
            },
        );
    }

    if let Some(type_desc) = column.type_desc.as_ref() {
        return build_create_tablet_column_pb_from_type_desc(type_desc, column_idx).map_err(
            |err| {
                format!(
                    "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing column_type and has unsupported type_desc (col_name={}): {}",
                    column_idx, column_name, err
                )
            },
        );
    }

    let slot = slot_descs_by_name
        .get(&column_name.to_ascii_lowercase())
        .ok_or_else(|| {
            format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing column_type/type_desc and no matching slot_desc by col_name={}",
                column_idx, column_name
            )
        })?;
    let slot_type = slot.slot_type.as_ref().ok_or_else(|| {
        format!(
            "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing column_type/type_desc and matched slot_desc has no slot_type (col_name={})",
            column_idx, column_name
        )
    })?;
    build_create_tablet_column_pb_from_type_desc(slot_type, column_idx).map_err(|err| {
        format!(
            "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing column_type/type_desc and matched slot_desc has unsupported slot_type (col_name={}): {}",
            column_idx, column_name, err
        )
    })
}

fn map_aggregation_type_to_schema_string(
    aggregation_type: Option<crate::types::TAggregationType>,
    is_key: bool,
    keys_type: KeysType,
    column_idx: usize,
) -> Result<Option<String>, String> {
    if is_key {
        return Ok(None);
    }
    if aggregation_type.is_none() && keys_type == KeysType::DupKeys {
        return Ok(None);
    }
    let agg = aggregation_type.ok_or_else(|| {
        format!(
            "missing aggregation_type for value column in schema.indexes column index {}",
            column_idx
        )
    })?;
    let name = match agg {
        crate::types::TAggregationType::SUM => "SUM",
        crate::types::TAggregationType::MAX => "MAX",
        crate::types::TAggregationType::MIN => "MIN",
        crate::types::TAggregationType::REPLACE => "REPLACE",
        crate::types::TAggregationType::HLL_UNION => "HLL_UNION",
        crate::types::TAggregationType::NONE => "NONE",
        crate::types::TAggregationType::BITMAP_UNION => "BITMAP_UNION",
        crate::types::TAggregationType::REPLACE_IF_NOT_NULL => "REPLACE_IF_NOT_NULL",
        crate::types::TAggregationType::PERCENTILE_UNION => "PERCENTILE_UNION",
        crate::types::TAggregationType::AGG_STATE_UNION => "AGG_STATE_UNION",
        other => {
            return Err(format!(
                "unsupported aggregation_type for value column in schema.indexes column index {}: {:?}",
                column_idx, other
            ));
        }
    };
    Ok(Some(name.to_string()))
}

fn map_primitive_to_starrocks_type(
    primitive: crate::types::TPrimitiveType,
) -> Option<&'static str> {
    let t = primitive;
    Some(if t == crate::types::TPrimitiveType::BOOLEAN {
        "BOOLEAN"
    } else if t == crate::types::TPrimitiveType::TINYINT {
        "TINYINT"
    } else if t == crate::types::TPrimitiveType::SMALLINT {
        "SMALLINT"
    } else if t == crate::types::TPrimitiveType::INT {
        "INT"
    } else if t == crate::types::TPrimitiveType::BIGINT {
        "BIGINT"
    } else if t == crate::types::TPrimitiveType::LARGEINT {
        "LARGEINT"
    } else if t == crate::types::TPrimitiveType::FLOAT {
        "FLOAT"
    } else if t == crate::types::TPrimitiveType::DOUBLE {
        "DOUBLE"
    } else if t == crate::types::TPrimitiveType::DATE {
        "DATE"
    } else if t == crate::types::TPrimitiveType::DATETIME {
        "DATETIME"
    } else if t == crate::types::TPrimitiveType::TIME {
        "DATETIME"
    } else if t == crate::types::TPrimitiveType::CHAR {
        "CHAR"
    } else if t == crate::types::TPrimitiveType::VARCHAR {
        "VARCHAR"
    } else if t == crate::types::TPrimitiveType::HLL {
        "HLL"
    } else if t == crate::types::TPrimitiveType::OBJECT {
        "OBJECT"
    } else if t == crate::types::TPrimitiveType::PERCENTILE {
        "PERCENTILE"
    } else if t == crate::types::TPrimitiveType::BINARY {
        "BINARY"
    } else if t == crate::types::TPrimitiveType::VARBINARY {
        "VARBINARY"
    } else if t == crate::types::TPrimitiveType::DECIMAL
        || t == crate::types::TPrimitiveType::DECIMALV2
    {
        // Native writer path is DecimalV3-based; map legacy decimal primitives to Decimal128.
        "DECIMAL128"
    } else if t == crate::types::TPrimitiveType::DECIMAL32 {
        "DECIMAL32"
    } else if t == crate::types::TPrimitiveType::DECIMAL64 {
        "DECIMAL64"
    } else if t == crate::types::TPrimitiveType::DECIMAL128 {
        "DECIMAL128"
    } else if t == crate::types::TPrimitiveType::DECIMAL256 {
        "DECIMAL256"
    } else if t == crate::types::TPrimitiveType::JSON {
        "JSON"
    } else {
        return None;
    })
}

#[cfg(test)]
mod tests {
    use super::map_primitive_to_starrocks_type;

    #[test]
    fn create_tablet_map_supports_largeint() {
        assert_eq!(
            map_primitive_to_starrocks_type(crate::types::TPrimitiveType::LARGEINT),
            Some("LARGEINT")
        );
    }

    #[test]
    fn create_tablet_map_supports_hll() {
        assert_eq!(
            map_primitive_to_starrocks_type(crate::types::TPrimitiveType::HLL),
            Some("HLL")
        );
    }

    #[test]
    fn create_tablet_map_supports_object() {
        assert_eq!(
            map_primitive_to_starrocks_type(crate::types::TPrimitiveType::OBJECT),
            Some("OBJECT")
        );
    }

    #[test]
    fn create_tablet_map_supports_percentile() {
        assert_eq!(
            map_primitive_to_starrocks_type(crate::types::TPrimitiveType::PERCENTILE),
            Some("PERCENTILE")
        );
    }

    #[test]
    fn create_tablet_map_supports_decimal256() {
        assert_eq!(
            map_primitive_to_starrocks_type(crate::types::TPrimitiveType::DECIMAL256),
            Some("DECIMAL256")
        );
    }
}
