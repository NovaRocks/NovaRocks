use crate::common::ids::SlotId;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::exec::chunk::field_slot_id;
use crate::formats::starrocks::cache as native_cache;
use crate::formats::starrocks::data::build_native_record_batch;
use crate::formats::starrocks::metadata::{load_bundle_segment_footers, load_tablet_snapshot};
use crate::formats::starrocks::plan::build_native_read_plan;
use crate::formats::starrocks::writer::read_bundle_parquet_snapshot_if_any;
use crate::novarocks_logging::{info, warn};
use arrow::array::{Array, ArrayRef, Int32Builder, LargeStringArray, ListArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::op::QueryGlobalDictEncodeMap;

pub(super) struct StarRocksNativeReader {
    tablet_id: i64,
    version: i64,
    next_batch: Option<RecordBatch>,
}

const NATIVE_BATCH_CACHE_MAX_ROWS: u64 = 200_000;

fn schema_signature(schema: &SchemaRef) -> String {
    schema
        .fields()
        .iter()
        .map(|field| {
            let metadata = if field.metadata().is_empty() {
                String::new()
            } else {
                let ordered = field
                    .metadata()
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect::<BTreeMap<_, _>>();
                format!("{ordered:?}")
            };
            format!(
                "{}:{:?}:{}:{}",
                field.name(),
                field.data_type(),
                field.is_nullable(),
                metadata
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

impl StarRocksNativeReader {
    pub(super) fn open(
        tablet_id: i64,
        tablet_root_path: &str,
        version: i64,
        _required_schema: SchemaRef,
        output_schema: SchemaRef,
        query_global_dicts: QueryGlobalDictEncodeMap,
        min_max_predicates: Vec<crate::formats::parquet::MinMaxPredicate>,
        object_store_profile: &ObjectStoreProfile,
    ) -> Result<Self, String> {
        let use_batch_cache = query_global_dicts.is_empty();
        let output_schema_sig = schema_signature(&output_schema);
        if use_batch_cache {
            if let Some(batch) = native_cache::native_batch_cache_get(
                tablet_root_path,
                tablet_id,
                version,
                &output_schema_sig,
            ) {
                return Ok(Self {
                    tablet_id,
                    version,
                    next_batch: Some(batch),
                });
            }
        }
        let (scan_schema, has_dict_encoded_output) =
            build_scan_schema_for_global_dict_encoding(&output_schema, &query_global_dicts)?;

        let snapshot = match load_tablet_snapshot(
            tablet_id,
            version,
            tablet_root_path,
            Some(object_store_profile),
        ) {
            Ok(snapshot) => snapshot,
            Err(err)
                if should_treat_missing_tablet_metadata_as_empty(
                    tablet_root_path,
                    version,
                    &err,
                ) =>
            {
                warn!(
                    "starrocks native reader degrades missing tablet metadata to empty batch: tablet_id={} version={} path={} error={}",
                    tablet_id, version, tablet_root_path, err
                );
                return Ok(Self {
                    tablet_id,
                    version,
                    next_batch: Some(RecordBatch::new_empty(output_schema.clone())),
                });
            }
            Err(err) => return Err(err),
        };
        let cacheable_small_snapshot = snapshot.total_num_rows <= NATIVE_BATCH_CACHE_MAX_ROWS;
        if let Some(batch) = read_bundle_parquet_snapshot_if_any(&snapshot, scan_schema.clone())? {
            let batch = if has_dict_encoded_output {
                encode_batch_with_query_global_dicts(batch, &output_schema, &query_global_dicts)?
            } else {
                batch
            };
            if use_batch_cache && cacheable_small_snapshot {
                native_cache::native_batch_cache_put(
                    tablet_root_path,
                    tablet_id,
                    version,
                    &output_schema_sig,
                    batch.clone(),
                );
            }
            return Ok(Self {
                tablet_id,
                version,
                next_batch: Some(batch),
            });
        }
        let segment_footers =
            load_bundle_segment_footers(&snapshot, tablet_root_path, Some(object_store_profile))?;
        let plan = build_native_read_plan(&snapshot, &segment_footers, &scan_schema)?;
        if let Some(first_footer) = segment_footers.first() {
            let column_debug = first_footer
                .columns
                .iter()
                .map(|c| {
                    format!(
                        "uid={:?},type={:?},enc={:?},comp={:?},ord_root={:?},ord_root_is_data={:?}",
                        c.unique_id,
                        c.logical_type,
                        c.encoding,
                        c.compression,
                        c.ordinal_index_root_page
                            .as_ref()
                            .map(|p| format!("{}:{}", p.offset, p.size)),
                        c.ordinal_index_root_is_data_page
                    )
                })
                .collect::<Vec<_>>()
                .join(" | ");
            info!(
                "starrocks rust_native first segment footer summary: tablet_id={}, version={}, columns=[{}]",
                tablet_id, version, column_debug
            );
        }
        let batch = build_native_record_batch(
            &plan,
            &segment_footers,
            tablet_root_path,
            Some(object_store_profile),
            &scan_schema,
            if cacheable_small_snapshot {
                &[]
            } else {
                &min_max_predicates
            },
        )
        .map_err(|e| {
            format!(
                "starrocks rust_native reader open failed in native data path (tablet_id={}, version={}, segment_count={}, projected_columns={}, estimated_rows={}): {}",
                plan.tablet_id,
                plan.version,
                plan.segments.len(),
                plan.projected_columns.len(),
                plan.estimated_rows,
                e
            )
        })?;
        let batch = if has_dict_encoded_output {
            encode_batch_with_query_global_dicts(batch, &output_schema, &query_global_dicts)?
        } else {
            batch
        };
        if use_batch_cache && cacheable_small_snapshot {
            native_cache::native_batch_cache_put(
                tablet_root_path,
                tablet_id,
                version,
                &output_schema_sig,
                batch.clone(),
            );
        }
        Ok(Self {
            tablet_id,
            version,
            next_batch: Some(batch),
        })
    }

    pub(super) fn get_next(
        &mut self,
        _output_schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>, String> {
        Ok(self.next_batch.take())
    }

    pub(super) fn close(&mut self) -> Result<(), String> {
        let _ = (self.tablet_id, self.version);
        Ok(())
    }
}

fn should_treat_missing_tablet_metadata_as_empty(
    tablet_root_path: &str,
    version: i64,
    error: &str,
) -> bool {
    if version == 1 && is_missing_tablet_metadata_error(error) {
        return true;
    }

    // If metadata lookup falls back all the way to version 1 and still cannot find
    // the tablet page/file, this tablet has never materialized metadata in the
    // shared bundle lineage. Treat it as an empty tablet for read compatibility.
    if is_missing_tablet_metadata_error(error) && error.contains("_0000000000000001.meta") {
        return true;
    }

    let path = tablet_root_path.to_ascii_lowercase();
    if !path.contains("/db10001/") && !path.contains("db10001/") {
        return false;
    }
    is_missing_tablet_metadata_error(error)
}

fn is_missing_tablet_metadata_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("metadata file not found:")
        || lowered.contains("bundle metadata does not contain tablet page:")
        || lowered.contains("bundle metadata missing tablet page for tablet_id=")
}

fn is_integer_dict_code_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn build_scan_schema_for_global_dict_encoding(
    output_schema: &SchemaRef,
    query_global_dicts: &QueryGlobalDictEncodeMap,
) -> Result<(SchemaRef, bool), String> {
    if query_global_dicts.is_empty() {
        return Ok((output_schema.clone(), false));
    }
    let mut fields = Vec::with_capacity(output_schema.fields().len());
    let mut changed = false;
    for field_ref in output_schema.fields() {
        let field = field_ref.as_ref();
        let slot_id = field_slot_id(field)?;
        let needs_dict_encode = slot_id
            .and_then(|slot| query_global_dicts.get(&slot))
            .is_some();
        if needs_dict_encode {
            if let Some(scan_type) = dict_scan_data_type_for_output(field.data_type()) {
                changed = true;
                info!(
                    "starrocks native dict scan type rewrite: field={} slot_id={:?} output_type={:?} scan_type={:?}",
                    field.name(),
                    slot_id,
                    field.data_type(),
                    scan_type
                );
                fields.push(Arc::new(field.clone().with_data_type(scan_type)));
                continue;
            }
            info!(
                "starrocks native dict scan type rewrite skipped: field={} slot_id={:?} output_type={:?}",
                field.name(),
                slot_id,
                field.data_type()
            );
        }
        fields.push(field_ref.clone());
    }
    if !changed {
        return Ok((output_schema.clone(), false));
    }
    let scan_schema = Arc::new(Schema::new_with_metadata(
        fields,
        output_schema.metadata().clone(),
    ));
    Ok((scan_schema, true))
}

fn encode_batch_with_query_global_dicts(
    scan_batch: RecordBatch,
    output_schema: &SchemaRef,
    query_global_dicts: &QueryGlobalDictEncodeMap,
) -> Result<RecordBatch, String> {
    if query_global_dicts.is_empty() {
        return Ok(scan_batch);
    }
    if scan_batch.num_columns() != output_schema.fields().len() {
        return Err(format!(
            "native starrocks dict encode output column mismatch: scan_columns={}, output_columns={}",
            scan_batch.num_columns(),
            output_schema.fields().len()
        ));
    }
    let mut arrays = Vec::with_capacity(scan_batch.num_columns());
    for (idx, field_ref) in output_schema.fields().iter().enumerate() {
        let output_field = field_ref.as_ref();
        let slot_id = field_slot_id(output_field)?;
        let Some(slot_id) = slot_id else {
            arrays.push(scan_batch.column(idx).clone());
            continue;
        };
        let Some(dict_map) = query_global_dicts.get(&slot_id) else {
            arrays.push(scan_batch.column(idx).clone());
            continue;
        };
        let encoded = encode_column_to_dict_ids(
            scan_batch.column(idx),
            output_field.data_type(),
            dict_map,
            output_field.name(),
            slot_id,
        )?;
        arrays.push(encoded);
    }
    RecordBatch::try_new(output_schema.clone(), arrays)
        .map_err(|e| format!("build dict-encoded native starrocks batch failed: {e}"))
}

fn encode_utf8_column_to_dict_ids(
    array: &ArrayRef,
    output_type: &DataType,
    dict_map: &HashMap<Vec<u8>, i32>,
    output_name: &str,
    slot_id: SlotId,
) -> Result<ArrayRef, String> {
    let mut builder = Int32Builder::with_capacity(array.len());
    let mut non_null_count = 0usize;
    let mut miss_count = 0usize;
    match array.data_type() {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast Utf8 array for dict encode failed".to_string())?;
            for row in 0..values.len() {
                if values.is_null(row) {
                    builder.append_null();
                } else {
                    non_null_count += 1;
                    let code = dict_map
                        .get(values.value(row).as_bytes())
                        .copied()
                        .unwrap_or_else(|| {
                            miss_count += 1;
                            0
                        });
                    builder.append_value(code);
                }
            }
        }
        DataType::LargeUtf8 => {
            let values = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "downcast LargeUtf8 array for dict encode failed".to_string())?;
            for row in 0..values.len() {
                if values.is_null(row) {
                    builder.append_null();
                } else {
                    non_null_count += 1;
                    let code = dict_map
                        .get(values.value(row).as_bytes())
                        .copied()
                        .unwrap_or_else(|| {
                            miss_count += 1;
                            0
                        });
                    builder.append_value(code);
                }
            }
        }
        other => {
            return Err(format!(
                "native starrocks dict encode expects Utf8 source column: slot_id={}, output_column={}, source_type={:?}",
                slot_id, output_name, other
            ));
        }
    }
    if non_null_count > 0 {
        info!(
            "starrocks global dict encode stats: slot_id={} output_column={} non_null={} miss={} dict_size={}",
            slot_id,
            output_name,
            non_null_count,
            miss_count,
            dict_map.len()
        );
    }
    let encoded_i32: ArrayRef = Arc::new(builder.finish());
    if output_type == &DataType::Int32 {
        return Ok(encoded_i32);
    }
    cast(encoded_i32.as_ref(), output_type).map_err(|e| {
        format!(
            "cast dict-encoded column to output type failed: slot_id={}, output_column={}, output_type={:?}, error={}",
            slot_id, output_name, output_type, e
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{QueryGlobalDictEncodeMap, encode_batch_with_query_global_dicts, schema_signature};
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;

    #[test]
    fn schema_signature_distinguishes_slot_metadata() {
        let schema_a = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("v2", DataType::Utf8, false),
            SlotId::new(2),
        )]));
        let schema_b = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("v2", DataType::Utf8, false),
            SlotId::new(4),
        )]));
        let sig_a = schema_signature(&schema_a);
        let sig_b = schema_signature(&schema_b);
        assert_ne!(
            sig_a, sig_b,
            "slot metadata must be part of cache signature"
        );
    }

    #[test]
    fn encode_batch_with_query_global_dicts_maps_utf8_to_ids() {
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("v1", DataType::Int32, true),
            SlotId::new(7),
        )]));
        let scan_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("v1", DataType::Utf8, true),
            SlotId::new(7),
        )]));
        let scan_batch = RecordBatch::try_new(
            scan_schema,
            vec![Arc::new(StringArray::from(vec![
                Some("a"),
                Some("x"),
                None,
            ]))],
        )
        .expect("scan batch");
        let mut dict_values = HashMap::new();
        dict_values.insert(b"a".to_vec(), 11);
        let mut dict_map = QueryGlobalDictEncodeMap::new();
        dict_map.insert(SlotId::new(7), Arc::new(dict_values));

        let encoded =
            encode_batch_with_query_global_dicts(scan_batch, &schema, &dict_map).expect("encode");
        let values = encoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        assert_eq!(values.value(0), 11);
        assert_eq!(values.value(1), 0);
        assert!(values.is_null(2));
    }
}

fn dict_scan_data_type_for_output(output_type: &DataType) -> Option<DataType> {
    if is_integer_dict_code_type(output_type) {
        return Some(DataType::Utf8);
    }
    match output_type {
        DataType::List(item) => {
            let scan_item = dict_scan_data_type_for_output(item.data_type())?;
            Some(DataType::List(Arc::new(
                item.as_ref().clone().with_data_type(scan_item),
            )))
        }
        _ => None,
    }
}

fn encode_column_to_dict_ids(
    array: &ArrayRef,
    output_type: &DataType,
    dict_map: &HashMap<Vec<u8>, i32>,
    output_name: &str,
    slot_id: SlotId,
) -> Result<ArrayRef, String> {
    if is_integer_dict_code_type(output_type) {
        return encode_utf8_column_to_dict_ids(array, output_type, dict_map, output_name, slot_id);
    }

    match output_type {
        DataType::List(output_item) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                format!(
                    "native starrocks dict encode expects ListArray for output column '{}' (slot_id={}), got {:?}",
                    output_name,
                    slot_id,
                    array.data_type()
                )
            })?;
            let encoded_values = encode_column_to_dict_ids(
                &list.values().clone(),
                output_item.data_type(),
                dict_map,
                output_name,
                slot_id,
            )?;
            Ok(Arc::new(ListArray::new(
                output_item.clone(),
                list.offsets().clone(),
                encoded_values,
                list.nulls().cloned(),
            )))
        }
        _ => Ok(array.clone()),
    }
}
