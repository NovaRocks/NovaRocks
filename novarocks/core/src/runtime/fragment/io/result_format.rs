use arrow::array::ArrayRef;
use novarocks_types::PrimitiveType;
use novarocks_types::arrow_primitive::arrow_field_to_primitive;

use crate::common::result_batch::ResultBatch;
use crate::common::util::{
    FieldRenderSchema, http_json_row_from_arrays_with_primitives,
    mysql_text_row_from_arrays_with_primitives,
};
use crate::exec::chunk::Chunk;

use super::{ResultPresentation, ResultProjection};

fn columns_for_projections(
    chunk: &Chunk,
    projections: &[ResultProjection],
) -> Result<Vec<ArrayRef>, String> {
    projections
        .iter()
        .map(|projection| chunk.column_by_slot_id(projection.slot_id()))
        .collect()
}

fn primitives_for_projections(projections: &[ResultProjection]) -> Vec<PrimitiveType> {
    projections
        .iter()
        .map(|projection| projection.primitive())
        .collect()
}

fn field_schemas_for_projections(projections: &[ResultProjection]) -> Vec<FieldRenderSchema> {
    projections
        .iter()
        .map(|projection| projection.field_schema().clone())
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

fn field_schemas_for_chunk_fields(chunk: &Chunk) -> Vec<FieldRenderSchema> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| FieldRenderSchema::from_field(slot.field()))
        .collect()
}

const STATISTIC_DATA_VERSION_V1: i32 = 1;

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

fn required_i32_field(
    fields: &[Option<Vec<u8>>],
    index: usize,
    field_name: &str,
) -> Result<i32, String> {
    let raw = fields
        .get(index)
        .ok_or_else(|| format!("missing field {field_name} at column {index}"))?
        .as_deref()
        .ok_or_else(|| format!("field {field_name} at column {index} is NULL"))?;
    let text = std::str::from_utf8(raw)
        .map_err(|error| format!("field {field_name} is not valid UTF-8: {error}"))?;
    text.parse::<i32>()
        .map_err(|error| format!("field {field_name} parse i32 failed: {error}"))
}

/// Builds the neutral Statistic batch envelope. The role adapter supplies the
/// protocol-specific row encoder.
pub fn build_statistic_result_batch(
    chunk: &Chunk,
    projections: &[ResultProjection],
    encoder: fn(i32, &[Option<Vec<u8>>]) -> Result<Vec<u8>, String>,
) -> Result<ResultBatch, String> {
    if projections.is_empty() {
        return Err("STATISTIC result sink requires non-empty projections".to_string());
    }

    let columns = columns_for_projections(chunk, projections)?;
    let primitives = primitives_for_projections(projections);
    let field_schemas = field_schemas_for_projections(projections);
    let mut batch = ResultBatch::empty();
    for row in 0..chunk.len() {
        let mysql_row = mysql_text_row_from_arrays_with_primitives(
            &columns,
            row,
            Some(&primitives),
            Some(&field_schemas),
        )?;
        let fields = parse_lenenc_fields(&mysql_row, columns.len())?;
        let version = required_i32_field(&fields, 0, "version")?;
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
        batch.rows.push(encoder(version, &fields)?);
    }
    if batch.statistic_version.is_none() {
        batch.statistic_version = Some(STATISTIC_DATA_VERSION_V1);
    }
    Ok(batch)
}

pub fn build_result_batch(
    chunk: &Chunk,
    projections: Option<&[ResultProjection]>,
    presentation: ResultPresentation,
) -> Result<ResultBatch, String> {
    if presentation == ResultPresentation::Statistic {
        return Err(
            "STATISTIC result presentation is owned by the Compat result adapter".to_string(),
        );
    }

    let (columns, primitives, field_schemas) = match projections.filter(|value| !value.is_empty()) {
        Some(projections) => (
            columns_for_projections(chunk, projections)?,
            primitives_for_projections(projections),
            field_schemas_for_projections(projections),
        ),
        None => (
            chunk.columns().to_vec(),
            primitives_for_chunk_fields(chunk),
            field_schemas_for_chunk_fields(chunk),
        ),
    };
    let mut batch = ResultBatch::empty();
    for row in 0..chunk.len() {
        let encoded = match presentation {
            ResultPresentation::MysqlText => mysql_text_row_from_arrays_with_primitives(
                &columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?,
            ResultPresentation::HttpJson => http_json_row_from_arrays_with_primitives(
                &columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?,
            ResultPresentation::Statistic => unreachable!("checked above"),
        };
        batch.rows.push(encoded);
    }
    Ok(batch)
}

pub fn empty_result_batch(presentation: ResultPresentation) -> Result<ResultBatch, String> {
    match presentation {
        ResultPresentation::MysqlText | ResultPresentation::HttpJson => Ok(ResultBatch::empty()),
        ResultPresentation::Statistic => {
            Err("STATISTIC result presentation is owned by the Compat result adapter".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    use super::build_statistic_result_batch;
    use crate::common::ids::SlotId;
    use crate::common::util::FieldRenderSchema;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::runtime::fragment::io::ResultProjection;
    use novarocks_types::PrimitiveType;

    fn statistic_chunk(versions: Vec<i32>) -> Chunk {
        let rows = versions.len();
        let payloads = (0..rows)
            .map(|index| format!("payload-{index}"))
            .collect::<Vec<_>>();
        let schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    Field::new("version", DataType::Int32, false),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    SlotId::new(2),
                    Field::new("payload", DataType::Utf8, true),
                    None,
                    None,
                ),
            ])
            .expect("chunk schema"),
        );
        Chunk::try_new_with_columns(
            schema,
            vec![
                Arc::new(Int32Array::from(versions)) as ArrayRef,
                Arc::new(StringArray::from(payloads)) as ArrayRef,
            ],
        )
        .expect("chunk")
    }

    fn projections() -> Vec<ResultProjection> {
        vec![
            ResultProjection::new(
                SlotId::new(1),
                PrimitiveType::Int,
                FieldRenderSchema::scalar(Some(PrimitiveType::Int)),
            ),
            ResultProjection::new(
                SlotId::new(2),
                PrimitiveType::Varchar,
                FieldRenderSchema::scalar(Some(PrimitiveType::Varchar)),
            ),
        ]
    }

    fn test_encoder(version: i32, fields: &[Option<Vec<u8>>]) -> Result<Vec<u8>, String> {
        let payload = fields
            .get(1)
            .and_then(|value| value.as_deref())
            .ok_or_else(|| "missing payload".to_string())?;
        Ok(format!("{version}:{}", String::from_utf8_lossy(payload)).into_bytes())
    }

    #[test]
    fn statistic_result_batch_encodes_single_and_multiple_rows() {
        let projections = projections();
        let batch =
            build_statistic_result_batch(&statistic_chunk(vec![1, 1]), &projections, test_encoder)
                .expect("statistic batch");

        assert_eq!(batch.statistic_version, Some(1));
        assert_eq!(
            batch.rows,
            vec![b"1:payload-0".to_vec(), b"1:payload-1".to_vec()]
        );
    }

    #[test]
    fn statistic_result_batch_rejects_mixed_versions() {
        let projections = projections();
        let error =
            build_statistic_result_batch(&statistic_chunk(vec![1, 2]), &projections, test_encoder)
                .expect_err("mixed versions must fail");

        assert_eq!(error, "mixed statistic versions in one batch: 1 vs 2");
    }

    #[test]
    fn statistic_result_batch_requires_projections() {
        let error = build_statistic_result_batch(&statistic_chunk(vec![1]), &[], test_encoder)
            .expect_err("missing projections must fail");

        assert_eq!(
            error,
            "STATISTIC result sink requires non-empty projections"
        );
    }

    #[test]
    fn statistic_result_batch_preserves_encoder_errors() {
        let projections = projections();
        let error =
            build_statistic_result_batch(&statistic_chunk(vec![1]), &projections, |_, _| {
                Err("row encoder failed".to_string())
            })
            .expect_err("encoder failure must propagate");

        assert_eq!(error, "row encoder failed");
    }

    #[test]
    fn statistic_result_batch_uses_the_legacy_default_for_empty_input() {
        let projections = projections();
        let batch =
            build_statistic_result_batch(&statistic_chunk(vec![]), &projections, test_encoder)
                .expect("empty statistic batch");

        assert_eq!(batch.statistic_version, Some(1));
        assert!(batch.rows.is_empty());
    }
}
