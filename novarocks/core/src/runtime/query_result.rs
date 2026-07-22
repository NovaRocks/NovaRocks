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

//! Generic query result types for standalone SQL execution.
//!
//! These types live here (rather than in `crate::engine`) so that
//! executors and coordinators under `crate::runtime` can reference the
//! result type without creating a dependency on the standalone engine module.

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::{Chunk, ChunkSchema};
use novarocks_catalog::schema::SqlType;

#[derive(Clone, Debug)]
pub struct QueryResultColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub logical_type: Option<SqlType>,
}

#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<QueryResultColumn>,
    pub chunks: Vec<Chunk>,
}

pub(crate) fn record_batch_to_chunk(batch: RecordBatch) -> Result<Chunk, String> {
    let slot_ids = (1..=batch.num_columns())
        .map(|idx| {
            u32::try_from(idx)
                .map(crate::common::ids::SlotId::new)
                .map_err(|_| "too many output columns".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

pub(crate) fn build_string_query_result(
    column_name: &str,
    rows: Vec<String>,
) -> Result<QueryResult, String> {
    let column = QueryResultColumn {
        name: column_name.to_string(),
        data_type: DataType::Utf8,
        nullable: false,
        logical_type: None,
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(
            rows.into_iter().map(Some).collect::<Vec<_>>(),
        ))],
    )
    .map_err(|e| format!("build standalone text result failed: {e}"))?;
    Ok(QueryResult {
        columns: vec![column],
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.chunks.iter().map(Chunk::len).sum()
    }

    pub fn into_chunks(self) -> Vec<Chunk> {
        self.chunks
    }

    /// Empty schema, empty chunks. Used as the no-op output when an
    /// IVM branch (insert or delete) has zero input files / rows.
    pub(crate) fn empty() -> Self {
        Self {
            columns: Vec::new(),
            chunks: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::common::ids::SlotId;

    #[test]
    fn record_batch_to_chunk_preserves_batch_and_assigns_one_based_slots() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        let expected = batch.clone();

        let chunk = record_batch_to_chunk(batch).unwrap();

        assert_eq!(chunk.batch, expected);
        assert_eq!(chunk.slot_id_to_index().get(&SlotId::new(1)), Some(&0));
        assert_eq!(chunk.slot_id_to_index().get(&SlotId::new(2)), Some(&1));
    }

    #[test]
    fn record_batch_to_chunk_accepts_zero_column_zero_row_batch() {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let chunk = record_batch_to_chunk(batch).unwrap();
        assert_eq!(chunk.len(), 0);
        assert!(chunk.slot_id_to_index().is_empty());
    }

    #[test]
    fn build_string_query_result_preserves_metadata_order_and_empty_rows() {
        let result = build_string_query_result(
            "Explain String",
            vec!["first".to_string(), "second".to_string()],
        )
        .unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "Explain String");
        assert_eq!(result.columns[0].data_type, DataType::Utf8);
        assert!(!result.columns[0].nullable);
        assert!(result.columns[0].logical_type.is_none());
        assert_eq!(result.row_count(), 2);
        let field = result.chunks[0].schema().field(0).clone();
        assert_eq!(field.name(), "Explain String");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(!field.is_nullable());
        let values = result.chunks[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "first");
        assert_eq!(values.value(1), "second");

        let empty = build_string_query_result("status", Vec::new()).unwrap();
        assert_eq!(empty.row_count(), 0);
        assert_eq!(empty.chunks.len(), 1);
        assert_eq!(empty.columns[0].name, "status");
    }

    #[test]
    fn query_result_column_preserves_logical_decimal_type() {
        let column = QueryResultColumn {
            name: "amount".to_string(),
            data_type: DataType::Decimal128(38, -2),
            nullable: true,
            logical_type: Some(SqlType::Decimal {
                precision: 38,
                scale: -2,
            }),
        };

        assert_eq!(column.name, "amount");
        assert_eq!(column.data_type, DataType::Decimal128(38, -2));
        assert!(column.nullable);
        assert_eq!(
            column.logical_type,
            Some(SqlType::Decimal {
                precision: 38,
                scale: -2,
            })
        );
    }
}
