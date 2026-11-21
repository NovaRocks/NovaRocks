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

//! Metrics for registering an existing Parquet file.

use std::collections::HashMap;

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::owned_resources::hashmap::fresh_table_layout;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaData;

use crate::file_reader::field_identity::ResolvedParquetLeaves;
use crate::iceberg::spec::{Datum, PrimitiveType, Schema, Type};
use crate::typed_read::column_handle::corrupt;

use self::convert::{Conversion, Value};
use self::truncate::BoundBytes;
use self::trust::{known_nulls, no_ordered_values, usable_statistics};

mod convert;
mod truncate;
mod trust;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) struct FooterMetrics {
    pub(crate) record_count: u64,
    pub(crate) column_sizes: HashMap<i32, u64>,
    pub(crate) value_counts: HashMap<i32, u64>,
    pub(crate) null_value_counts: HashMap<i32, u64>,
    pub(crate) lower_bounds: HashMap<i32, Datum>,
    pub(crate) upper_bounds: HashMap<i32, Datum>,
}

/// Allocation facts derived without keeping an owned copy of any bound or map.
/// The caller reserves the original statement's bytes before materializing.
pub(crate) struct FooterMetricsPlan<'a> {
    metadata: &'a ParquetMetaData,
    leaves: &'a ResolvedParquetLeaves,
    table_schema: &'a Schema,
    record_count: u64,
    column_size_entries: usize,
    value_entries: usize,
    null_entries: usize,
    lower_entries: usize,
    upper_entries: usize,
    retained_bytes: usize,
}

impl FooterMetricsPlan<'_> {
    /// Map tables and bound backing moved into the DataFile builder. This
    /// excludes the inline DataFile and any storage owned by the caller.
    pub(crate) fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// The caller must keep the allocation receipt with the resulting backing.
    pub(crate) fn materialize(self) -> Result<FooterMetrics, ConnectorError> {
        let mut metrics = FooterMetrics {
            record_count: self.record_count,
            column_sizes: metric_map(self.column_size_entries)?,
            value_counts: metric_map(self.value_entries)?,
            null_value_counts: metric_map(self.null_entries)?,
            lower_bounds: metric_map(self.lower_entries)?,
            upper_bounds: metric_map(self.upper_entries)?,
        };
        for (ordinal, leaf) in self.leaves.iter().enumerate() {
            let Some(id) = leaf.field_id else { continue };
            let column =
                column_metrics(self.metadata, self.table_schema, ordinal, id, leaf.repeated)?;
            if let Some(size) = column.size {
                metrics.column_sizes.insert(id, size);
            }
            if let Some(value_count) = column.value_count {
                metrics.value_counts.insert(id, value_count);
            }
            if let Some(null_count) = column.null_count {
                metrics.null_value_counts.insert(id, null_count);
            }
            if let Some(bounds) = column.bounds {
                metrics
                    .lower_bounds
                    .insert(id, as_datum(bounds.lower, bounds.data_type.clone())?);
                if let Some(upper) = bounds.upper {
                    metrics
                        .upper_bounds
                        .insert(id, as_datum(upper, bounds.data_type)?);
                }
            }
        }
        Ok(metrics)
    }
}

pub(crate) fn analyze_footer_metrics<'a>(
    metadata: &'a ParquetMetaData,
    leaves: &'a ResolvedParquetLeaves,
    table_schema: &'a Schema,
) -> Result<FooterMetricsPlan<'a>, ConnectorError> {
    let num_columns = metadata.file_metadata().schema_descr().num_columns();
    if num_columns != leaves.len()
        || metadata
            .row_groups()
            .iter()
            .any(|group| group.columns().len() != num_columns)
    {
        return Err(corrupt(
            "Parquet metric leaf ordinals disagree with the footer",
        ));
    }
    let record_count = metadata.row_groups().iter().try_fold(0, |total, group| {
        add_counts(total, group.num_rows(), "row count")
    })?;
    let mut plan = FooterMetricsPlan {
        metadata,
        leaves,
        table_schema,
        record_count,
        column_size_entries: 0,
        value_entries: 0,
        null_entries: 0,
        lower_entries: 0,
        upper_entries: 0,
        retained_bytes: 0,
    };
    for (ordinal, leaf) in leaves.iter().enumerate() {
        let Some(id) = leaf.field_id else { continue };
        let column = column_metrics(metadata, table_schema, ordinal, id, leaf.repeated)?;
        for (entry_count, count) in [
            (&mut plan.column_size_entries, column.size),
            (&mut plan.value_entries, column.value_count),
            (&mut plan.null_entries, column.null_count),
        ] {
            if count.is_some() {
                *entry_count += 1;
            }
        }
        if let Some(bounds) = column.bounds {
            for (entry_count, bytes) in [
                (&mut plan.lower_entries, Some(bounds.lower)),
                (&mut plan.upper_entries, bounds.upper),
            ] {
                let Some(bytes) = bytes else { continue };
                *entry_count += 1;
                let length = bytes.as_slice().len();
                if matches!(
                    bounds.data_type,
                    PrimitiveType::String | PrimitiveType::Binary | PrimitiveType::Fixed(_)
                ) {
                    plan.retained_bytes = checked_backing_add(plan.retained_bytes, length)?;
                }
            }
        }
    }
    for count in [
        plan.column_size_entries,
        plan.value_entries,
        plan.null_entries,
    ] {
        plan.retained_bytes = checked_backing_add(plan.retained_bytes, map_bytes::<u64>(count)?)?;
    }
    for count in [plan.lower_entries, plan.upper_entries] {
        plan.retained_bytes = checked_backing_add(plan.retained_bytes, map_bytes::<Datum>(count)?)?;
    }
    Ok(plan)
}

#[derive(Default)]
struct ColumnMetrics<'a> {
    size: Option<u64>,
    value_count: Option<u64>,
    null_count: Option<u64>,
    bounds: Option<ColumnBounds<'a>>,
}

struct ColumnBounds<'a> {
    data_type: PrimitiveType,
    lower: BoundBytes<'a>,
    upper: Option<BoundBytes<'a>>,
}

fn column_metrics<'a>(
    metadata: &'a ParquetMetaData,
    table_schema: &Schema,
    ordinal: usize,
    id: i32,
    repeated: bool,
) -> Result<ColumnMetrics<'a>, ConnectorError> {
    let descriptor = &metadata.file_metadata().schema_descr().columns()[ordinal];
    let int96 = descriptor.physical_type() == PhysicalType::INT96;
    let target = table_schema
        .field_by_id(id)
        .map(|field| field.field_type.as_ref());
    let conversion = match target {
        Some(Type::Primitive(target)) if !repeated && !int96 => {
            Conversion::new(descriptor, Some(target))
        }
        None if !repeated && !int96 => Conversion::new(descriptor, None),
        _ => None,
    };
    let mut size = None;
    let mut value_count = 0;
    let mut null_count = Some(0);
    let mut counts_available = true;
    let mut ordered_bounds = conversion.is_some();
    let mut bound_values: Option<(Value<'a>, Value<'a>)> = None;
    for group in metadata.row_groups() {
        let column = &group.columns()[ordinal];
        size = Some(add_counts(
            size.unwrap_or_default(),
            column.compressed_size(),
            "column size",
        )?);
        if repeated {
            continue;
        }
        let group_values = add_counts(0, column.num_values(), "value count")?;
        value_count = add_counts(value_count, column.num_values(), "value count")?;
        let stats = column.statistics();
        let group_nulls = stats.and_then(|stats| known_nulls(stats, group_values));
        null_count = match (null_count, group_nulls) {
            (Some(total), Some(count)) => Some(add_counts(total, count as i64, "NULL count")?),
            _ => None,
        };
        if int96 {
            continue;
        }
        let Some(stats) = stats.filter(|stats| usable_statistics(stats, group_nulls)) else {
            counts_available = false;
            ordered_bounds = false;
            continue;
        };
        let Some(conversion) = conversion.as_ref().filter(|_| ordered_bounds) else {
            continue;
        };
        if no_ordered_values(stats, group_values, group_nulls) {
            continue;
        }
        if let Some((low, high)) = conversion.decode_bounds(stats) {
            bound_values = Some(match bound_values {
                Some((prev_low, prev_high)) => (prev_low.min(low), prev_high.max(high)),
                None => (low, high),
            });
        } else {
            ordered_bounds = false;
        }
    }
    if repeated || !counts_available {
        return Ok(ColumnMetrics {
            size,
            ..ColumnMetrics::default()
        });
    }
    let bounds = match (ordered_bounds, bound_values, conversion) {
        (true, Some((low, high)), Some(conversion)) => Some(ColumnBounds {
            lower: conversion
                .encode(low, false)
                .ok_or_else(|| corrupt("Parquet lower bound conversion changed its type"))?,
            upper: conversion.encode(high, true),
            data_type: conversion.output_type,
        }),
        _ => None,
    };
    // Java emits zero counts for empty ordinary leaves. Trino aggregates INT96
    // by occurrence, so no empty INT96 entry is synthesized.
    let has_counts = !int96 || size.is_some();
    Ok(ColumnMetrics {
        size,
        value_count: has_counts.then_some(value_count),
        null_count: null_count.filter(|_| has_counts),
        bounds,
    })
}

fn as_datum(bytes: BoundBytes<'_>, ty: PrimitiveType) -> Result<Datum, ConnectorError> {
    Datum::try_from_bytes(bytes.as_slice(), ty)
        .map_err(|_| corrupt("Parquet metric bound cannot be decoded as an Iceberg value"))
}

fn add_counts(total: u64, count: i64, what: &str) -> Result<u64, ConnectorError> {
    // Iceberg persists these counters as signed Avro longs.
    u64::try_from(count)
        .ok()
        .and_then(|count| total.checked_add(count))
        .filter(|total| *total <= i64::MAX as u64)
        .ok_or_else(|| {
            corrupt(format!(
                "Parquet metric {what} is negative or overflows Iceberg long"
            ))
        })
}

fn resource(detail: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, detail)
}

fn checked_backing_add(left: usize, right: usize) -> Result<usize, ConnectorError> {
    left.checked_add(right)
        .ok_or_else(|| resource("Parquet metric backing size overflow"))
}

fn map_bytes<T>(count: usize) -> Result<usize, ConnectorError> {
    fresh_table_layout::<i32, T>(count)
        .map(|table| table.request_bytes_upper_bound)
        .map_err(|error| resource(&error.to_string()))
}

fn metric_map<T>(count: usize) -> Result<HashMap<i32, T>, ConnectorError> {
    let mut map = HashMap::new();
    map.try_reserve(count)
        .map_err(|_| resource("Parquet metric map allocation failed"))?;
    Ok(map)
}
