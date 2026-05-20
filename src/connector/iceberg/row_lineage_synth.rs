//! V3 row-lineage column synthesis helpers.
//!
//! Iceberg V3 spec rule for reading `_row_id` and `_last_updated_sequence_number`
//! metadata columns:
//!   1. If the data file carries a stored column with the reserved field id and
//!      the value is non-NULL on a given row, use that stored value.
//!   2. Otherwise, fall back to `first_row_id + row_position` (for `_row_id`) or
//!      to the file's `data_sequence_number` (for `_last_updated_sequence_number`).
//!
//! The IVM `IcebergDeltaScan` reader and the regular base scan reader must both
//! follow this rule. This module centralises the implementation so all readers
//! produce identical row_id values for the same physical row.
//!
//! Cross-reference: iceberg-rust upstream
//! `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs::create_row_id_column`.

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::Schema;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::exec::row_position::{
    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, ICEBERG_RESERVED_FIELD_ID_ROW_ID,
};

/// Indices of stored row-lineage columns (`_row_id`, `_last_updated_seq`) in a
/// batch schema, if present.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StoredRowLineageIndices {
    pub(crate) row_id: Option<usize>,
    pub(crate) last_updated_seq: Option<usize>,
}

/// Locate stored row-lineage columns by their reserved Iceberg field ids in
/// the supplied Arrow schema. A column is considered "stored" iff its field
/// metadata `PARQUET:field_id` matches the reserved id.
pub(crate) fn stored_row_lineage_indices(schema: &Schema) -> StoredRowLineageIndices {
    let mut out = StoredRowLineageIndices::default();
    for (idx, field) in schema.fields().iter().enumerate() {
        let Some(field_id_str) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
            continue;
        };
        let Ok(field_id) = field_id_str.parse::<i32>() else {
            continue;
        };
        if field_id == ICEBERG_RESERVED_FIELD_ID_ROW_ID && out.row_id.is_none() {
            out.row_id = Some(idx);
        } else if field_id == ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
            && out.last_updated_seq.is_none()
        {
            out.last_updated_seq = Some(idx);
        }
    }
    out
}

/// Synthesize `_row_id` values for the rows currently in `columns`.
///
/// `positions` is the absolute row position of each row within its source data
/// file. When `None`, the rows are assumed to start at `0` and increment by 1.
pub(crate) fn synthesize_row_id(
    schema: &Schema,
    columns: &[ArrayRef],
    num_rows: usize,
    first_row_id: i64,
    positions: Option<&[i64]>,
) -> Result<Vec<i64>, String> {
    let idx = stored_row_lineage_indices(schema);
    let stored: Option<&Int64Array> = idx
        .row_id
        .map(|i| {
            columns
                .get(i)
                .ok_or_else(|| {
                    format!(
                        "row-lineage stored _row_id column index {i} out of bounds (columns.len={})",
                        columns.len()
                    )
                })
                .and_then(|col| {
                    col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        format!(
                            "stored _row_id column must be Int64, got {:?}",
                            col.data_type()
                        )
                    })
                })
        })
        .transpose()?;

    if let Some(p) = positions
        && p.len() != num_rows
    {
        return Err(format!(
            "synthesize_row_id positions.len()={} does not match num_rows={num_rows}",
            p.len()
        ));
    }

    let mut out = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        if let Some(arr) = stored
            && !arr.is_null(i)
        {
            out.push(arr.value(i));
            continue;
        }
        let position = match positions {
            Some(p) => p[i],
            None => i as i64,
        };
        let computed = first_row_id.checked_add(position).ok_or_else(|| {
            format!(
                "Row ID overflow when computing fallback _row_id: first_row_id={first_row_id}, position={position}"
            )
        })?;
        out.push(computed);
    }
    Ok(out)
}

/// Synthesize `_last_updated_sequence_number` values for the rows currently in
/// `columns`. Falls back to the file-level `data_sequence_number` when stored
/// values are missing or NULL.
pub(crate) fn synthesize_last_updated_sequence_number(
    schema: &Schema,
    columns: &[ArrayRef],
    num_rows: usize,
    data_sequence_number: i64,
) -> Result<Vec<i64>, String> {
    let idx = stored_row_lineage_indices(schema);
    let stored: Option<&Int64Array> = idx
        .last_updated_seq
        .map(|i| {
            columns
                .get(i)
                .ok_or_else(|| {
                    format!(
                        "row-lineage stored _last_updated_sequence_number index {i} out of bounds"
                    )
                })
                .and_then(|col| {
                    col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        format!(
                            "stored _last_updated_sequence_number column must be Int64, got {:?}",
                            col.data_type()
                        )
                    })
                })
        })
        .transpose()?;

    let mut out = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        if let Some(arr) = stored
            && !arr.is_null(i)
        {
            out.push(arr.value(i));
        } else {
            out.push(data_sequence_number);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn field_with_id(name: &str, id: i32, ty: DataType, nullable: bool) -> Field {
        let mut metadata = HashMap::new();
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        Field::new(name, ty, nullable).with_metadata(metadata)
    }

    fn schema_with_stored_row_id() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            field_with_id(
                "_row_id",
                ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                DataType::Int64,
                true,
            ),
            field_with_id(
                "_last_updated_sequence_number",
                ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                DataType::Int64,
                true,
            ),
        ])
    }

    #[test]
    fn locates_stored_row_lineage_columns_by_field_id() {
        let schema = schema_with_stored_row_id();
        let idx = stored_row_lineage_indices(&schema);
        assert_eq!(idx.row_id, Some(1));
        assert_eq!(idx.last_updated_seq, Some(2));
    }

    #[test]
    fn returns_none_when_stored_lineage_absent() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let idx = stored_row_lineage_indices(&schema);
        assert!(idx.row_id.is_none());
        assert!(idx.last_updated_seq.is_none());
    }

    #[test]
    fn synthesize_row_id_uses_stored_when_present_and_non_null() {
        let schema = schema_with_stored_row_id();
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![100i64, 200, 300]));
        let stored_row_id: ArrayRef = Arc::new(Int64Array::from(vec![Some(42i64), None, Some(7)]));
        let stored_seq: ArrayRef =
            Arc::new(Int64Array::from(vec![None as Option<i64>, None, None]));
        let columns = vec![id_col, stored_row_id, stored_seq];

        let row_ids = synthesize_row_id(&schema, &columns, 3, 1000, None).expect("synthesize ok");

        assert_eq!(row_ids, vec![42, 1001, 7]);
    }

    #[test]
    fn synthesize_row_id_falls_back_when_stored_column_absent() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![100i64, 200, 300]));
        let columns = vec![id_col];

        let row_ids = synthesize_row_id(&schema, &columns, 3, 1000, None).expect("synthesize ok");

        assert_eq!(row_ids, vec![1000, 1001, 1002]);
    }

    #[test]
    fn synthesize_row_id_honors_positions_when_provided() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![100i64, 200]));
        let columns = vec![id_col];

        let row_ids =
            synthesize_row_id(&schema, &columns, 2, 500, Some(&[3, 9])).expect("synthesize ok");

        assert_eq!(row_ids, vec![503, 509]);
    }

    #[test]
    fn synthesize_last_updated_seq_uses_stored_when_non_null() {
        let schema = schema_with_stored_row_id();
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![100i64, 200]));
        let stored_row_id: ArrayRef = Arc::new(Int64Array::from(vec![None as Option<i64>, None]));
        let stored_seq: ArrayRef = Arc::new(Int64Array::from(vec![Some(11i64), None]));
        let columns = vec![id_col, stored_row_id, stored_seq];

        let seqs = synthesize_last_updated_sequence_number(&schema, &columns, 2, 99)
            .expect("synthesize ok");

        assert_eq!(seqs, vec![11, 99]);
    }
}
