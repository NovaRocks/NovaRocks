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

use arrow::array::ArrayRef;

use crate::common::result_batch::ResultBatch;
use crate::common::util::{
    FieldRenderSchema, http_json_row_from_arrays_with_primitives,
    mysql_text_row_from_arrays_with_primitives,
};
use crate::exec::chunk::Chunk;
use crate::runtime::fragment::io::ResultProjection;
use novarocks_types::PrimitiveType;
use novarocks_types::arrow_primitive::arrow_field_to_primitive;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultSinkType {
    MySqlProtocol,
    HttpProtocol,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultSinkFormat {
    Json,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ResultSinkConfig {
    pub(crate) sink_type: ResultSinkType,
    pub(crate) format: Option<ResultSinkFormat>,
}

impl ResultSinkConfig {
    pub(crate) fn mysql() -> Self {
        Self {
            sink_type: ResultSinkType::MySqlProtocol,
            format: None,
        }
    }

    pub(crate) fn http_json() -> Self {
        Self {
            sink_type: ResultSinkType::HttpProtocol,
            format: Some(ResultSinkFormat::Json),
        }
    }
}

fn columns_for_projections(
    chunk: &Chunk,
    projections: &[ResultProjection],
) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(projections.len());
    for projection in projections {
        out.push(chunk.column_by_slot_id(projection.slot_id())?);
    }
    Ok(out)
}

fn primitives_for_projections(projections: &[ResultProjection]) -> Vec<PrimitiveType> {
    projections
        .iter()
        .map(ResultProjection::primitive)
        .collect()
}

fn primitives_for_chunk_fields(chunk: &Chunk) -> Vec<PrimitiveType> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| arrow_field_to_primitive(slot.field()).unwrap_or(PrimitiveType::Invalid))
        .collect()
}

fn field_schemas_for_projections(projections: &[ResultProjection]) -> Vec<FieldRenderSchema> {
    projections
        .iter()
        .map(|projection| projection.field_schema().clone())
        .collect()
}

fn field_schemas_for_chunk_fields(chunk: &Chunk) -> Vec<FieldRenderSchema> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| FieldRenderSchema::from_field(slot.field()))
        .collect()
}

pub(crate) fn build_empty_fetch_result_batch_template(
    config: ResultSinkConfig,
) -> Result<ResultBatch, String> {
    if config.sink_type == ResultSinkType::HttpProtocol {
        if config.format != Some(ResultSinkFormat::Json) {
            return Err(format!(
                "HTTP_PROTOCAL result sink only supports JSON format, got {:?}",
                config.format
            ));
        }
    }

    Ok(ResultBatch::empty())
}

pub(crate) fn build_fetch_result_batch_for_chunk(
    chunk: &Chunk,
    projections: Option<&[ResultProjection]>,
    config: ResultSinkConfig,
) -> Result<ResultBatch, String> {
    if config.sink_type == ResultSinkType::HttpProtocol {
        if config.format != Some(ResultSinkFormat::Json) {
            return Err(format!(
                "HTTP_PROTOCAL result sink only supports JSON format, got {:?}",
                config.format
            ));
        }

        let mut batch = ResultBatch::empty();
        if let Some(projections) = projections.filter(|v| !v.is_empty()) {
            let columns = columns_for_projections(chunk, projections)?;
            let primitives = primitives_for_projections(projections);
            let field_schemas = field_schemas_for_projections(projections);
            for row in 0..chunk.len() {
                batch.rows.push(http_json_row_from_arrays_with_primitives(
                    &columns,
                    row,
                    Some(&primitives),
                    Some(&field_schemas),
                )?);
            }
        } else {
            let columns = chunk.columns();
            let primitives = primitives_for_chunk_fields(chunk);
            let field_schemas = field_schemas_for_chunk_fields(chunk);
            for row in 0..chunk.len() {
                batch.rows.push(http_json_row_from_arrays_with_primitives(
                    columns,
                    row,
                    Some(&primitives),
                    Some(&field_schemas),
                )?);
            }
        }
        return Ok(batch);
    }

    let mut batch = ResultBatch::empty();
    if let Some(projections) = projections.filter(|v| !v.is_empty()) {
        let columns = columns_for_projections(chunk, projections)?;
        let primitives = primitives_for_projections(projections);
        let field_schemas = field_schemas_for_projections(projections);
        for row in 0..chunk.len() {
            let bytes = mysql_text_row_from_arrays_with_primitives(
                &columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?;
            batch.rows.push(bytes);
        }
    } else {
        let columns = chunk.columns();
        let primitives = primitives_for_chunk_fields(chunk);
        let field_schemas = field_schemas_for_chunk_fields(chunk);
        for row in 0..chunk.len() {
            let bytes = mysql_text_row_from_arrays_with_primitives(
                columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?;
            batch.rows.push(bytes);
        }
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BinaryArray, Int32Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Field};

    use super::build_fetch_result_batch_for_chunk;
    use super::{ResultProjection, ResultSinkConfig};
    use crate::common::ids::SlotId;
    use crate::common::util::FieldRenderSchema;
    use crate::exec::chunk::{Chunk, ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
    use novarocks_types::PrimitiveType;
    use novarocks_types::logical::{LogicalType, field_with_logical_type};

    fn chunk_with_stale_field_schema(field: Field, column: ArrayRef) -> Result<Chunk, String> {
        let chunk_schema = Arc::new(ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                field,
                Some(ChunkFieldSchema::empty()),
                None,
            ),
        ])?);
        Chunk::try_new_with_columns(chunk_schema, vec![column])
    }

    #[test]
    fn fetch_fallback_http_json_uses_arrow_field_metadata_for_json() {
        let field = field_with_logical_type(
            Field::new("payload", DataType::Utf8, true),
            LogicalType::Json,
        );
        let chunk = chunk_with_stale_field_schema(
            field,
            Arc::new(StringArray::from(vec![r#"{"a":1}"#])) as ArrayRef,
        )
        .expect("chunk");

        let batch = build_fetch_result_batch_for_chunk(&chunk, None, ResultSinkConfig::http_json())
            .expect("fetch batch");

        assert_eq!(batch.rows, vec![b"{\"data\":[{\"a\":1}]}\n".to_vec()]);
    }

    #[test]
    fn fetch_fallback_mysql_uses_arrow_field_metadata_for_opaque_binary() {
        let field =
            field_with_logical_type(Field::new("hll", DataType::Binary, true), LogicalType::Hll);
        let chunk = chunk_with_stale_field_schema(
            field,
            Arc::new(BinaryArray::from(vec![Some(b"opaque".as_slice())])) as ArrayRef,
        )
        .expect("chunk");

        let batch = build_fetch_result_batch_for_chunk(&chunk, None, ResultSinkConfig::mysql())
            .expect("fetch batch");

        assert_eq!(batch.rows, vec![vec![0xFB]]);
    }

    #[test]
    fn fetch_http_json_projection_uses_native_render_schema_for_nested_json() {
        let list_values = StringArray::from(vec![r#"{"k":1}"#, r#"{"k":2}"#]);
        let offsets =
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![0i32, 2]));
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            offsets,
            Arc::new(list_values),
            None,
        );
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    Field::new("id", DataType::Int32, false),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    SlotId::new(2),
                    Field::new(
                        "payloads",
                        DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                        true,
                    ),
                    Some(ChunkFieldSchema::empty()),
                    None,
                ),
            ])
            .expect("schema"),
        );
        let chunk = Chunk::try_new_with_columns(
            chunk_schema,
            vec![
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(list) as ArrayRef,
            ],
        )
        .expect("chunk");
        let projections = vec![ResultProjection::new(
            SlotId::new(2),
            PrimitiveType::Invalid,
            FieldRenderSchema::complex(vec![FieldRenderSchema::scalar(Some(PrimitiveType::Json))]),
        )];

        let batch = build_fetch_result_batch_for_chunk(
            &chunk,
            Some(&projections),
            ResultSinkConfig::http_json(),
        )
        .expect("fetch batch");

        assert_eq!(
            batch.rows,
            vec![b"{\"data\":[[{\"k\":1},{\"k\":2}]]}\n".to_vec()]
        );
    }
}
