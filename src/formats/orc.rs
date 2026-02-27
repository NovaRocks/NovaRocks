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
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use orc_rust::schema::RootDataType;

use crate::cache::DataCacheContext;
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_slot_id, field_with_slot_id};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::RuntimeFilterContext;
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::fs::scan_context::{FileScanContext, FileScanRange};
use crate::metrics;
use crate::runtime::profile::{RuntimeProfile, clamp_u128_to_i64};

const VIRTUAL_COUNT_COLUMN: &str = "___count___";

#[derive(Clone, Debug)]
pub struct OrcScanConfig {
    pub columns: Vec<String>,
    pub slot_ids: Vec<SlotId>,
    pub case_sensitive: bool,
    pub orc_use_column_names: bool,
    pub hive_column_names: Option<Vec<String>>,
    pub batch_size: Option<usize>,
    pub datacache: DataCacheContext,
}

pub fn build_orc_iter(
    scan: FileScanContext,
    cfg: OrcScanConfig,
    limit: Option<usize>,
    profile: Option<RuntimeProfile>,
    _runtime_filters: Option<&RuntimeFilterContext>,
) -> Result<BoxedExecIter, String> {
    if scan.ranges.is_empty() {
        return Ok(Box::new(std::iter::empty()));
    }
    let factory = scan.factory.with_datacache_context(cfg.datacache.clone());
    let iter = OrcScanIter::new(cfg, scan.ranges, factory, limit, profile);
    Ok(Box::new(iter))
}

#[derive(Clone, Debug)]
enum OutputColumn {
    Count,
    BatchIndex(usize),
}

struct OrcScanIter {
    cfg: OrcScanConfig,
    ranges: Vec<FileScanRange>,
    factory: OpendalRangeReaderFactory,
    range_idx: usize,
    reader: Option<orc_rust::arrow_reader::ArrowReader<crate::fs::opendal::OpendalRangeReader>>,
    output_columns: Option<Vec<OutputColumn>>,
    remaining: usize,
    profile: Option<RuntimeProfile>,
}

impl OrcScanIter {
    fn new(
        cfg: OrcScanConfig,
        ranges: Vec<FileScanRange>,
        factory: OpendalRangeReaderFactory,
        limit: Option<usize>,
        profile: Option<RuntimeProfile>,
    ) -> Self {
        let remaining = limit.unwrap_or(usize::MAX);
        Self {
            cfg,
            ranges,
            factory,
            range_idx: 0,
            reader: None,
            output_columns: None,
            remaining,
            profile,
        }
    }

    fn open_next_reader(&mut self) -> Result<bool, String> {
        if self.range_idx >= self.ranges.len() {
            return Ok(false);
        }
        let range = self.ranges[self.range_idx].clone();
        self.range_idx += 1;

        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("OrcRanges", metrics::TUnit::UNIT, 1);
        }

        let path = range.path.clone();
        let file_len = range.file_len;
        let len = (file_len > 0).then_some(file_len);
        let reader = self
            .factory
            .open_with_len(&path, len)
            .map(|r| {
                r.with_modification_time_override(
                    range
                        .external_datacache
                        .as_ref()
                        .and_then(|opts| opts.modification_time),
                )
            })
            .map_err(|e| e.to_string())?;

        let mut builder = ArrowReaderBuilder::try_new(reader).map_err(|e| e.to_string())?;
        let root = builder.file_metadata().root_data_type();
        let (projection, output_columns) = resolve_projection(&self.cfg, root)?;
        builder = builder.with_projection(projection);
        if let Some(bs) = self.cfg.batch_size {
            builder = builder.with_batch_size(bs);
        }

        if range.length > 0 {
            let mut end = range
                .offset
                .checked_add(range.length)
                .ok_or_else(|| "orc scan range overflow when computing end offset".to_string())?;
            if range.file_len > 0 && end > range.file_len {
                end = range.file_len;
            }
            if end < range.offset {
                return Err("orc scan range end < start".to_string());
            }
            let start = range.offset as usize;
            let end = end as usize;
            if end > start {
                builder = builder.with_file_byte_range(start..end);
            }
        }

        self.reader = Some(builder.build());
        self.output_columns = Some(output_columns);
        Ok(true)
    }
}

impl Iterator for OrcScanIter {
    type Item = Result<Chunk, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.remaining == 0 {
                return None;
            }
            if self.reader.is_none() {
                match self.open_next_reader() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            let reader = self.reader.as_mut().expect("orc reader");
            match reader.next() {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let output_columns = match self.output_columns.as_ref() {
                        Some(cols) => cols,
                        None => {
                            return Some(Err("orc scan missing output column mapping".to_string()));
                        }
                    };
                    let batch = match reorder_orc_batch(&self.cfg, output_columns, batch) {
                        Ok(batch) => batch,
                        Err(e) => return Some(Err(e)),
                    };

                    let to_take = std::cmp::min(batch.num_rows(), self.remaining);
                    let batch = if to_take < batch.num_rows() {
                        batch.slice(0, to_take)
                    } else {
                        batch
                    };
                    self.remaining -= to_take;
                    if let Some(profile) = self.profile.as_ref() {
                        profile.counter_add("OrcBatchesOut", metrics::TUnit::UNIT, 1);
                        profile.counter_add(
                            "OrcRowsOut",
                            metrics::TUnit::UNIT,
                            clamp_u128_to_i64(to_take as u128),
                        );
                        profile.counter_add(
                            "RawRowsRead",
                            metrics::TUnit::UNIT,
                            clamp_u128_to_i64(to_take as u128),
                        );
                    }
                    return Some(Ok(Chunk::new(batch)));
                }
                Some(Err(e)) => {
                    self.reader = None;
                    return Some(Err(e.to_string()));
                }
                None => {
                    self.reader = None;
                    self.output_columns = None;
                }
            }
        }
    }
}

fn resolve_projection(
    cfg: &OrcScanConfig,
    root: &RootDataType,
) -> Result<(ProjectionMask, Vec<OutputColumn>), String> {
    let mut output_positions: Vec<Option<usize>> = Vec::with_capacity(cfg.columns.len());
    let mut requested_positions = HashSet::new();

    let use_orc_column_names = cfg.orc_use_column_names
        || cfg
            .hive_column_names
            .as_ref()
            .is_none_or(|names| names.is_empty());
    for col in &cfg.columns {
        if col == VIRTUAL_COUNT_COLUMN {
            output_positions.push(None);
            continue;
        }
        let pos = if use_orc_column_names {
            find_root_pos_by_name(root, col, cfg.case_sensitive)?
        } else {
            find_root_pos_by_hive(cfg, col)?
        };
        output_positions.push(Some(pos));
        requested_positions.insert(pos);
    }

    if requested_positions.is_empty() {
        let output_columns = output_positions
            .into_iter()
            .map(|pos| match pos {
                None => OutputColumn::Count,
                Some(_) => OutputColumn::Count,
            })
            .collect();
        return Ok((ProjectionMask::all(), output_columns));
    }

    let mut projected_positions = Vec::new();
    for (idx, _) in root.children().iter().enumerate() {
        if requested_positions.contains(&idx) {
            projected_positions.push(idx);
        }
    }

    let mut pos_to_batch = HashMap::new();
    for (batch_idx, pos) in projected_positions.iter().enumerate() {
        pos_to_batch.insert(*pos, batch_idx);
    }

    let mut output_columns = Vec::with_capacity(output_positions.len());
    for pos in output_positions {
        let col = match pos {
            None => OutputColumn::Count,
            Some(root_pos) => {
                let batch_idx = *pos_to_batch.get(&root_pos).ok_or_else(|| {
                    format!("orc scan output column root_pos {} not projected", root_pos)
                })?;
                OutputColumn::BatchIndex(batch_idx)
            }
        };
        output_columns.push(col);
    }

    let mut column_indices = Vec::with_capacity(projected_positions.len());
    for pos in projected_positions {
        let col = root
            .children()
            .get(pos)
            .ok_or_else(|| format!("orc scan column position {} out of range", pos))?;
        column_indices.push(col.data_type().column_index());
    }

    Ok((ProjectionMask::roots(root, column_indices), output_columns))
}

fn find_root_pos_by_name(
    root: &RootDataType,
    name: &str,
    case_sensitive: bool,
) -> Result<usize, String> {
    for (idx, col) in root.children().iter().enumerate() {
        if case_sensitive {
            if col.name() == name {
                return Ok(idx);
            }
        } else if col.name().eq_ignore_ascii_case(name) {
            return Ok(idx);
        }
    }
    Err(format!("orc scan missing column in file schema: {}", name))
}

fn find_root_pos_by_hive(cfg: &OrcScanConfig, name: &str) -> Result<usize, String> {
    let Some(hive_cols) = cfg.hive_column_names.as_ref() else {
        return Err("orc scan requires hive_column_names for ordinal mapping".to_string());
    };
    for (idx, col_name) in hive_cols.iter().enumerate() {
        if cfg.case_sensitive {
            if col_name == name {
                return Ok(idx);
            }
        } else if col_name.eq_ignore_ascii_case(name) {
            return Ok(idx);
        }
    }
    Err(format!(
        "orc scan missing column in hive_column_names: {}",
        name
    ))
}

fn reorder_orc_batch(
    cfg: &OrcScanConfig,
    output_columns: &[OutputColumn],
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if output_columns.is_empty() {
        return Ok(batch);
    }

    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(output_columns.len());
    let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(output_columns.len());

    for (idx, out) in output_columns.iter().enumerate() {
        match out {
            OutputColumn::Count => {
                let row_count = batch.num_rows();
                let count_array: ArrayRef = Arc::new(BooleanArray::from(vec![true; row_count]));
                let count_field = Arc::new(Field::new(
                    cfg.columns
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| VIRTUAL_COUNT_COLUMN.to_string()),
                    DataType::Boolean,
                    false,
                ));
                new_columns.push(count_array);
                new_fields.push(count_field);
            }
            OutputColumn::BatchIndex(batch_idx) => {
                let column = batch.column(*batch_idx).clone();
                let field = batch.schema().field(*batch_idx).clone().into();
                new_columns.push(column);
                new_fields.push(field);
            }
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    let batch = RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e: arrow::error::ArrowError| e.to_string())?;
    attach_slot_ids_to_batch(cfg, batch)
}

fn attach_slot_ids_to_batch(
    cfg: &OrcScanConfig,
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if batch.num_columns() == 0 {
        return Ok(batch);
    }

    if cfg.slot_ids.is_empty() {
        return Err(format!(
            "orc scan missing slot_ids for non-empty batch: num_columns={}",
            batch.num_columns()
        ));
    }

    if batch.num_columns() != cfg.slot_ids.len() {
        return Err(format!(
            "orc scan output columns/slot_ids mismatch: num_columns={}, slot_ids={:?}",
            batch.num_columns(),
            cfg.slot_ids
        ));
    }

    let schema = batch.schema();
    let mut already_aligned = true;
    for (f, slot_id) in schema.fields().iter().zip(cfg.slot_ids.iter()) {
        let input_slot_id = field_slot_id(f.as_ref())?;
        if input_slot_id != Some(*slot_id) {
            already_aligned = false;
            break;
        }
    }
    if already_aligned {
        return Ok(batch);
    }

    let mut new_fields = Vec::with_capacity(schema.fields().len());
    for (f, slot_id) in schema.fields().iter().zip(cfg.slot_ids.iter()) {
        new_fields.push(Arc::new(field_with_slot_id((**f).clone(), *slot_id)));
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .map_err(|e: arrow::error::ArrowError| e.to_string())
}
