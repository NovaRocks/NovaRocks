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

use crate::common::ids::SlotId;
use crate::common::result_batch::ResultBatch;
use crate::common::util::{
    FieldRenderSchema, http_json_row_from_arrays_with_primitives,
    mysql_text_row_from_arrays_with_primitives,
};
use crate::exec::chunk::Chunk;
use crate::types::PrimitiveType;
use crate::types::arrow_primitive::arrow_field_to_primitive;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultSinkType {
    MySqlProtocol,
    HttpProtocol,
    Statistic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultSinkFormat {
    Json,
}

pub(crate) type StatisticRowEncoder =
    fn(version: i32, fields: &[Option<Vec<u8>>]) -> Result<Vec<u8>, String>;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ResultSinkConfig {
    pub(crate) sink_type: ResultSinkType,
    pub(crate) format: Option<ResultSinkFormat>,
    pub(crate) statistic_encoder: Option<StatisticRowEncoder>,
}

impl ResultSinkConfig {
    pub(crate) fn mysql() -> Self {
        Self {
            sink_type: ResultSinkType::MySqlProtocol,
            format: None,
            statistic_encoder: None,
        }
    }

    pub(crate) fn http_json() -> Self {
        Self {
            sink_type: ResultSinkType::HttpProtocol,
            format: Some(ResultSinkFormat::Json),
            statistic_encoder: None,
        }
    }

    pub(crate) fn statistic(encoder: StatisticRowEncoder) -> Self {
        Self {
            sink_type: ResultSinkType::Statistic,
            format: None,
            statistic_encoder: Some(encoder),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResultProjection {
    pub(crate) slot_id: SlotId,
    pub(crate) primitive: PrimitiveType,
    pub(crate) field_schema: FieldRenderSchema,
}

const STATISTIC_DATA_VERSION_V1: i32 = 1;

fn columns_for_projections(
    chunk: &Chunk,
    projections: &[ResultProjection],
) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(projections.len());
    for projection in projections {
        out.push(chunk.column_by_slot_id(projection.slot_id)?);
    }
    Ok(out)
}

fn primitives_for_projections(projections: &[ResultProjection]) -> Vec<PrimitiveType> {
    projections
        .iter()
        .map(|projection| projection.primitive)
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
        .map(|projection| projection.field_schema.clone())
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

fn parse_lenenc_fields(
    row: &[u8],
    expected_columns: usize,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut fields = Vec::with_capacity(expected_columns);
    let mut cursor = 0usize;
    while fields.len() < expected_columns {
        let marker = *row
            .get(cursor)
            .ok_or_else(|| "mysql text row ended unexpectedly".to_string())?;
        cursor += 1;

        if marker == 0xFB {
            fields.push(None);
            continue;
        }

        let len = if marker < 0xFB {
            marker as usize
        } else if marker == 0xFC {
            let bytes = row
                .get(cursor..cursor + 2)
                .ok_or_else(|| "mysql text row invalid 0xFC length".to_string())?;
            cursor += 2;
            u16::from_le_bytes([bytes[0], bytes[1]]) as usize
        } else if marker == 0xFD {
            let bytes = row
                .get(cursor..cursor + 3)
                .ok_or_else(|| "mysql text row invalid 0xFD length".to_string())?;
            cursor += 3;
            (bytes[0] as usize) | ((bytes[1] as usize) << 8) | ((bytes[2] as usize) << 16)
        } else if marker == 0xFE {
            let bytes = row
                .get(cursor..cursor + 8)
                .ok_or_else(|| "mysql text row invalid 0xFE length".to_string())?;
            cursor += 8;
            u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]) as usize
        } else {
            return Err(format!(
                "mysql text row invalid length marker 0x{marker:02x}"
            ));
        };

        let value = row
            .get(cursor..cursor + len)
            .ok_or_else(|| "mysql text row value length exceeds payload".to_string())?;
        cursor += len;
        fields.push(Some(value.to_vec()));
    }
    if cursor != row.len() {
        return Err("mysql text row has trailing bytes".to_string());
    }
    Ok(fields)
}

fn field_bytes<'a>(
    fields: &'a [Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<&'a [u8]>, String> {
    let value = fields
        .get(idx)
        .ok_or_else(|| format!("missing field {field_name} at column {idx}"))?;
    Ok(value.as_deref())
}

fn field_required_i32(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<i32, String> {
    let raw = field_bytes(fields, idx, field_name)?
        .ok_or_else(|| format!("field {field_name} at column {idx} is NULL"))?;
    let text = std::str::from_utf8(raw)
        .map_err(|e| format!("field {field_name} is not valid UTF-8: {e}"))?;
    text.parse::<i32>()
        .map_err(|e| format!("field {field_name} parse i32 failed: {e}"))
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

    let mut batch = ResultBatch::empty();
    if config.sink_type == ResultSinkType::Statistic {
        if config.statistic_encoder.is_none() {
            return Err("STATISTIC result sink requires statistic encoder".to_string());
        }
        batch.statistic_version = Some(STATISTIC_DATA_VERSION_V1);
    }
    Ok(batch)
}

pub(crate) fn build_fetch_result_batch_for_chunk(
    chunk: &Chunk,
    projections: Option<&[ResultProjection]>,
    config: ResultSinkConfig,
) -> Result<ResultBatch, String> {
    if config.sink_type == ResultSinkType::Statistic {
        let encoder = config
            .statistic_encoder
            .ok_or_else(|| "STATISTIC result sink requires statistic encoder".to_string())?;
        let projections = projections
            .filter(|v| !v.is_empty())
            .ok_or_else(|| "STATISTIC result sink requires non-empty projections".to_string())?;
        let mut batch = ResultBatch::empty();
        let columns = columns_for_projections(chunk, projections)?;
        let primitives = primitives_for_projections(projections);
        let field_schemas = field_schemas_for_projections(projections);
        for row in 0..chunk.len() {
            let mysql_row = mysql_text_row_from_arrays_with_primitives(
                &columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?;
            let fields = parse_lenenc_fields(&mysql_row, columns.len())?;
            let version = field_required_i32(&fields, 0, "version")?;
            if let Some(existing) = batch.statistic_version {
                if existing != version {
                    return Err(format!(
                        "mixed statistic versions in one batch: {} vs {}",
                        existing, version
                    ));
                }
            } else {
                batch.statistic_version = Some(version);
            }
            let encoded = encoder(version, &fields)?;
            batch.rows.push(encoded);
        }
        if batch.statistic_version.is_none() {
            batch.statistic_version = Some(STATISTIC_DATA_VERSION_V1);
        }
        return Ok(batch);
    }

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
    use crate::types::PrimitiveType;
    use crate::types::logical::{LogicalType, field_with_logical_type};

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
        let projections = vec![ResultProjection {
            slot_id: SlotId::new(2),
            primitive: PrimitiveType::Invalid,
            field_schema: FieldRenderSchema::complex(vec![FieldRenderSchema::scalar(Some(
                PrimitiveType::Json,
            ))]),
        }];

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
