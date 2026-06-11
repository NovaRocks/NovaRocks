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
use crate::exec::operators::sort::{
    ChunksSorter, append_stable_row_index_sort_column, concat_sort_chunks, normalize_sort_key_array,
};

use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};

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
        let batch = concat_sort_chunks(chunks)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        if self.order_by.is_empty() {
            return Chunk::try_new_like(batch, &chunks[0])
                .map(Some)
                .map_err(|e| e.to_string());
        }

        let chunk = Chunk::new_like(batch.clone(), &chunks[0]);
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
        append_stable_row_index_sort_column(&mut sort_columns, batch.num_rows());
        let indices = lexsort_to_indices(&sort_columns, None).map_err(|e| e.to_string())?;
        let columns = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let sorted = arrow::record_batch::RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| e.to_string())?;
        Chunk::try_new_like(sorted, &chunks[0])
            .map(Some)
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;

    use arrow::array::{Array, Decimal128Array, Int32Array, make_array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn make_chunk(values: Vec<Option<i32>>, nullable: bool) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            nullable,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])
            .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn make_chunk_with_decimal_payload(ids: Vec<i32>, prices: Vec<i128>) -> Chunk {
        let wide_prices = Decimal128Array::from(prices.into_iter().map(Some).collect::<Vec<_>>())
            .with_precision_and_scale(38, 2)
            .expect("wide decimal prices");
        let price_data = wide_prices
            .to_data()
            .into_builder()
            .data_type(DataType::Decimal128(20, 2))
            .build()
            .expect("decimal20 price data");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("price", DataType::Decimal128(20, 2), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(ids)), make_array(price_data)],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn full_sort_reconciles_nullable_columns_across_chunks() {
        let chunks = vec![
            make_chunk(vec![Some(1), Some(2)], false),
            make_chunk(vec![None, Some(3)], true),
        ];
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let sorter = ChunksSorterFullSort::new(
            Arc::new(arena),
            vec![SortExpression {
                expr,
                asc: true,
                nulls_first: true,
            }],
        );

        let out = sorter.sort_chunks(&chunks).expect("sort").expect("chunk");
        assert!(out.batch.schema().field(0).is_nullable());
        let col = out
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32");
        assert_eq!(col.len(), 4);
        assert!(col.is_null(0));
        assert_eq!(col.value(1), 1);
        assert_eq!(col.value(2), 2);
        assert_eq!(col.value(3), 3);
    }

    #[test]
    fn full_sort_preserves_decimal_payload_exceeding_declared_precision() {
        let overflow = 100_000_000_000_000_000_000_i128;
        let chunks = vec![make_chunk_with_decimal_payload(
            vec![2, 1],
            vec![overflow, 12_345_i128],
        )];
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let sorter = ChunksSorterFullSort::new(
            Arc::new(arena),
            vec![SortExpression {
                expr,
                asc: true,
                nulls_first: true,
            }],
        );

        let out = sorter.sort_chunks(&chunks).expect("sort").expect("chunk");
        let ids = out
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id int32");
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        let prices = out
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("price decimal");
        assert_eq!(prices.data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(prices.value(0), 12_345_i128);
        assert!(!prices.is_null(1));
        assert_eq!(prices.value(1), overflow);
    }
}
