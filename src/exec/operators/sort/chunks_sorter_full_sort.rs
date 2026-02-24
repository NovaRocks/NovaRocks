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
//! Full-sort chunks sorter.
//!
//! This sorter materializes all input chunks, applies ORDER BY semantics,
//! and returns one globally sorted chunk.

use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::sort::SortExpression;
use crate::exec::operators::sort::{ChunksSorter, normalize_sort_key_array};

use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};

/// Full sort implementation used by `SORT` mode.
pub(crate) struct ChunksSorterFullSort {
    arena: Arc<ExprArena>,
    order_by: Vec<SortExpression>,
}

impl ChunksSorterFullSort {
    pub(crate) fn new(arena: Arc<ExprArena>, order_by: Vec<SortExpression>) -> Self {
        Self { arena, order_by }
    }
}

impl ChunksSorter for ChunksSorterFullSort {
    fn sort_chunks(&self, chunks: &[Chunk]) -> Result<Option<Chunk>, String> {
        if chunks.is_empty() {
            return Ok(None);
        }
        let schema = chunks[0].schema();
        let batches: Vec<_> = chunks.iter().map(|c| c.batch.clone()).collect();
        let batch = concat_batches(&schema, &batches).map_err(|e| e.to_string())?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        if self.order_by.is_empty() {
            return Chunk::try_new(batch).map(Some).map_err(|e| e.to_string());
        }

        let chunk = Chunk::new(batch.clone());
        let mut sort_columns = Vec::with_capacity(self.order_by.len());
        for sort_expr in &self.order_by {
            let values = self
                .arena
                .eval(sort_expr.expr, &chunk)
                .map_err(|e| e.to_string())?;
            let values = normalize_sort_key_array(&values)?;
            sort_columns.push(SortColumn {
                values,
                options: Some(SortOptions {
                    descending: !sort_expr.asc,
                    nulls_first: sort_expr.nulls_first,
                }),
            });
        }
        let indices = lexsort_to_indices(&sort_columns, None).map_err(|e| e.to_string())?;
        let columns = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let sorted = arrow::record_batch::RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| e.to_string())?;
        Chunk::try_new(sorted).map(Some).map_err(|e| e.to_string())
    }
}
