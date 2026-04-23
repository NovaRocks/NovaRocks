//! Aggregate-table row merging.
//!
//! Collapses duplicate-key rows per `ColumnAggregation` (SUM / MIN / MAX /
//! REPLACE) before handing the merged rows back to the caller. Used by the
//! iceberg insert path for aggregate-keyed tables.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;

use crate::sql::catalog::ColumnDef;
use crate::sql::parser::ast::{ColumnAggregation, Literal, TableKeyDesc, TableKeyKind};
use crate::standalone::engine::catalog::normalize_identifier;
use crate::standalone::engine::sqlparse::expr::{
    LiteralKey, compare_literals, literal_from_batch, literal_to_key,
};

pub(crate) fn merge_aggregate_table_rows_if_needed(
    columns: &[ColumnDef],
    key_desc: Option<&TableKeyDesc>,
    column_aggregations: &HashMap<String, ColumnAggregation>,
    batch: &RecordBatch,
) -> Result<Option<Vec<Vec<Literal>>>, String> {
    let Some(key_desc) = key_desc else {
        return Ok(None);
    };
    if key_desc.kind != TableKeyKind::Aggregate || batch.num_rows() <= 1 {
        return Ok(None);
    }

    let mut column_index_by_name = HashMap::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        column_index_by_name.insert(normalize_identifier(&column.name)?, idx);
    }
    let key_indices = key_desc
        .columns
        .iter()
        .map(|column| {
            let key = normalize_identifier(column)?;
            column_index_by_name
                .get(&key)
                .copied()
                .ok_or_else(|| format!("aggregate key column `{column}` not found in table schema"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut merged_rows = Vec::<Vec<Literal>>::new();
    let mut row_index_by_key = HashMap::<Vec<LiteralKey>, usize>::new();
    for row_idx in 0..batch.num_rows() {
        let row = (0..batch.num_columns())
            .map(|col_idx| literal_from_batch(batch.column(col_idx), row_idx))
            .collect::<Result<Vec<_>, _>>()?;
        let key = key_indices
            .iter()
            .map(|idx| literal_to_key(&row[*idx]))
            .collect::<Vec<_>>();
        if let Some(existing_idx) = row_index_by_key.get(&key).copied() {
            let existing = merged_rows
                .get_mut(existing_idx)
                .ok_or_else(|| "aggregate key merge state is inconsistent".to_string())?;
            merge_aggregate_table_row(existing, &row, &key_indices, columns, column_aggregations)?;
        } else {
            row_index_by_key.insert(key, merged_rows.len());
            merged_rows.push(row);
        }
    }
    Ok(Some(merged_rows))
}

fn merge_aggregate_table_row(
    existing: &mut [Literal],
    incoming: &[Literal],
    key_indices: &[usize],
    columns: &[ColumnDef],
    column_aggregations: &HashMap<String, ColumnAggregation>,
) -> Result<(), String> {
    for (column_idx, (existing_value, incoming_value)) in
        existing.iter_mut().zip(incoming.iter()).enumerate()
    {
        if key_indices.contains(&column_idx) {
            continue;
        }
        let column = columns
            .get(column_idx)
            .ok_or_else(|| "aggregate table column index is out of bounds".to_string())?;
        let key = normalize_identifier(&column.name)?;
        let aggregation = column_aggregations
            .get(&key)
            .copied()
            .unwrap_or(ColumnAggregation::Replace);
        merge_aggregate_table_value(existing_value, incoming_value, aggregation)?;
    }
    Ok(())
}

fn merge_aggregate_table_value(
    existing: &mut Literal,
    incoming: &Literal,
    aggregation: ColumnAggregation,
) -> Result<(), String> {
    match aggregation {
        ColumnAggregation::Sum => match (existing.clone(), incoming) {
            (_, Literal::Null) => Ok(()),
            (Literal::Null, other) => {
                *existing = other.clone();
                Ok(())
            }
            (Literal::Int(left), Literal::Int(right)) => {
                *existing = Literal::Int(
                    left.checked_add(*right)
                        .ok_or_else(|| format!("aggregate SUM overflow: {left} + {right}"))?,
                );
                Ok(())
            }
            (Literal::Float(left), Literal::Float(right)) => {
                *existing = Literal::Float(left + right);
                Ok(())
            }
            (Literal::Float(left), Literal::Int(right)) => {
                *existing = Literal::Float(left + (*right as f64));
                Ok(())
            }
            (Literal::Int(left), Literal::Float(right)) => {
                *existing = Literal::Float((left as f64) + right);
                Ok(())
            }
            (left, right) => Err(format!(
                "aggregate SUM does not support values {:?} and {:?}",
                left, right
            )),
        },
        ColumnAggregation::Min => {
            if matches!(incoming, Literal::Null) {
                return Ok(());
            }
            if matches!(existing, Literal::Null)
                || compare_literals(incoming, existing)? == std::cmp::Ordering::Less
            {
                *existing = incoming.clone();
            }
            Ok(())
        }
        ColumnAggregation::Max => {
            if matches!(incoming, Literal::Null) {
                return Ok(());
            }
            if matches!(existing, Literal::Null)
                || compare_literals(incoming, existing)? == std::cmp::Ordering::Greater
            {
                *existing = incoming.clone();
            }
            Ok(())
        }
        ColumnAggregation::Replace => {
            *existing = incoming.clone();
            Ok(())
        }
    }
}
