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

//! Core correctness adapter for connector-neutral ORC physical batches.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

use novarocks_execution::exec::chunk::ChunkSchemaRef;
use novarocks_fs::DataCacheContext;

const VIRTUAL_COUNT_COLUMN: &str = "___count___";

#[derive(Clone, Debug)]
pub struct OrcScanConfig {
    pub columns: Vec<String>,
    pub chunk_schema: ChunkSchemaRef,
    pub case_sensitive: bool,
    pub orc_use_column_names: bool,
    pub hive_column_names: Option<Vec<String>>,
    pub batch_size: Option<usize>,
    pub datacache: DataCacheContext,
}

pub(crate) fn adapt_foundation_batch(
    cfg: &OrcScanConfig,
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    let batch_schema = batch.schema();
    let mut columns = Vec::with_capacity(cfg.columns.len());
    let mut fields = Vec::with_capacity(cfg.columns.len());
    for column in &cfg.columns {
        if column == VIRTUAL_COUNT_COLUMN {
            columns.push(Arc::new(BooleanArray::from(vec![true; batch.num_rows()])) as ArrayRef);
            fields.push(Arc::new(Field::new(column, DataType::Boolean, false)));
            continue;
        }
        let index = if cfg.orc_use_column_names {
            if cfg.case_sensitive {
                batch_schema.index_of(column).ok()
            } else {
                batch_schema
                    .fields()
                    .iter()
                    .position(|field| field.name().eq_ignore_ascii_case(column))
            }
        } else {
            cfg.hive_column_names.as_ref().and_then(|names| {
                names.iter().position(|name| {
                    if cfg.case_sensitive {
                        name == column
                    } else {
                        name.eq_ignore_ascii_case(column)
                    }
                })
            })
        }
        .ok_or_else(|| format!("ORC physical batch missing requested column: {column}"))?;
        columns.push(batch.column(index).clone());
        fields.push(batch_schema.field(index).clone().into());
    }
    validate_batch_slot_count(
        cfg,
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| error.to_string())?,
    )
}

fn validate_batch_slot_count(
    cfg: &OrcScanConfig,
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if batch.num_columns() == 0 {
        return Ok(batch);
    }
    if cfg.chunk_schema.slot_ids().is_empty() {
        return Err(format!(
            "orc scan missing chunk schema for non-empty batch: num_columns={}",
            batch.num_columns()
        ));
    }
    if batch.num_columns() != cfg.chunk_schema.slot_ids().len() {
        return Err(format!(
            "orc scan output columns/chunk schema mismatch: num_columns={}, slot_ids={:?}",
            batch.num_columns(),
            cfg.chunk_schema.slot_ids()
        ));
    }
    Ok(batch)
}
