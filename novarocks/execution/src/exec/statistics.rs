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

//! Bounded, fragment-local statistics collection.
//!
//! The collector accepts Arrow batches from one fragment and produces a
//! versioned partial payload. Provider evidence, query lifecycle aggregation,
//! and publication deliberately remain outside this module.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datasketches::theta::ThetaSketch;
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES, StatisticsMetric, StatisticsMetricRequest,
};
use sha2::{Digest, Sha256};

pub const MAX_STATISTICS_THETA_RETAINED_HASHES: usize = 1 << 12;
const THETA_PARTIAL_WIRE_VERSION: u8 = 1;
const THETA_PARTIAL_WIRE_HEADER_BYTES: usize = 1 + 1 + 8 + 4;
const STATISTICS_FRAGMENT_PAYLOAD_VERSION: u8 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsFragmentError {
    message: String,
}

impl StatisticsFragmentError {
    fn contract(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn exhausted(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for StatisticsFragmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsFragmentError {}

#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsScalarPartial {
    row_count: u64,
    null_count: u64,
    total_size: u64,
    minimum: Option<StatisticsScalarBound>,
    maximum: Option<StatisticsScalarBound>,
}

#[derive(Clone, Debug, PartialEq)]
enum StatisticsScalarBound {
    F64(f64),
    LargeInt(i128),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThetaSketchPartial {
    lg_k: u8,
    theta: u64,
    retained_hashes: Vec<u64>,
}

/// A bounded partial which can be transferred in one terminal fragment report.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatisticsFragmentPartial {
    table: Option<StatisticsScalarPartial>,
    columns: BTreeMap<Arc<str>, StatisticsScalarPartial>,
    theta: BTreeMap<Arc<str>, ThetaSketchPartial>,
}

/// Per-fragment Arrow collector. It owns no connector/provider or query state.
pub struct StatisticsBatchCollector {
    schema: SchemaRef,
    metrics: StatisticsMetricRequest,
    column_indexes: BTreeMap<Arc<str>, usize>,
    table_rows: u64,
    columns: BTreeMap<Arc<str>, StatisticsScalarAccumulator>,
    theta: BTreeMap<Arc<str>, StatisticsThetaAccumulator>,
}

#[derive(Clone, Debug, Default)]
struct StatisticsScalarAccumulator {
    row_count: u64,
    null_count: u64,
    total_size: u64,
    minimum: Option<StatisticsScalarBound>,
    maximum: Option<StatisticsScalarBound>,
}

#[derive(Debug)]
struct StatisticsThetaAccumulator {
    sketch: ThetaSketch,
}

impl StatisticsBatchCollector {
    pub fn try_new(
        schema: SchemaRef,
        metrics: StatisticsMetricRequest,
    ) -> Result<Self, StatisticsFragmentError> {
        let mut column_indexes = BTreeMap::new();
        for metric in metrics.metrics() {
            let Some(column) = statistics_metric_column(metric) else {
                continue;
            };
            let index = schema
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    StatisticsFragmentError::contract(format!(
                        "statistics scan schema does not contain requested column `{column}`"
                    ))
                })?;
            column_indexes.insert(column.clone(), index);
        }
        let scalar_columns = metrics
            .metrics()
            .iter()
            .filter_map(|metric| match metric {
                StatisticsMetric::NullCount { column }
                | StatisticsMetric::Minimum { column }
                | StatisticsMetric::Maximum { column }
                | StatisticsMetric::AverageSize { column } => Some(column.clone()),
                StatisticsMetric::RowCount | StatisticsMetric::ThetaNdv { .. } => None,
            })
            .collect::<BTreeSet<_>>();
        let theta_columns = metrics
            .metrics()
            .iter()
            .filter_map(|metric| match metric {
                StatisticsMetric::ThetaNdv { column } => Some(column.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        Ok(Self {
            schema,
            metrics,
            column_indexes,
            table_rows: 0,
            columns: scalar_columns
                .into_iter()
                .map(|column| (column, StatisticsScalarAccumulator::default()))
                .collect(),
            theta: theta_columns
                .into_iter()
                .map(|column| (column, StatisticsThetaAccumulator::new(12)))
                .collect(),
        })
    }

    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<(), StatisticsFragmentError> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(StatisticsFragmentError::contract(
                "statistics batch schema differs from the pinned scan schema",
            ));
        }
        let rows = u64::try_from(batch.num_rows()).map_err(|_| {
            StatisticsFragmentError::exhausted("statistics batch row count exceeds u64")
        })?;
        self.table_rows = self
            .table_rows
            .checked_add(rows)
            .ok_or_else(|| StatisticsFragmentError::exhausted("statistics row count overflow"))?;
        for (column, index) in &self.column_indexes {
            let array = batch.column(*index);
            if let Some(accumulator) = self.columns.get_mut(column) {
                accumulator.push(array, rows)?;
            }
            if let Some(accumulator) = self.theta.get_mut(column) {
                accumulator.push(array)?;
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<StatisticsFragmentPartial, StatisticsFragmentError> {
        let table = StatisticsScalarPartial::try_new_bounds(self.table_rows, 0, 0, None, None)?;
        let mut partial = StatisticsFragmentPartial::default().with_table(table);
        for (column, accumulator) in self.columns {
            partial = partial.with_column(column, accumulator.finish()?);
        }
        for (column, accumulator) in self.theta {
            partial = partial.with_theta(column, accumulator.finish()?);
        }
        debug_assert!(!self.metrics.metrics().is_empty());
        Ok(partial)
    }

    pub fn finish_fragment_payload(self) -> Result<Bytes, StatisticsFragmentError> {
        self.finish()?.to_payload()
    }
}

impl StatisticsFragmentPartial {
    pub fn with_table(mut self, partial: StatisticsScalarPartial) -> Self {
        self.table = Some(partial);
        self
    }

    pub fn with_column(
        mut self,
        column: impl Into<Arc<str>>,
        partial: StatisticsScalarPartial,
    ) -> Self {
        self.columns.insert(column.into(), partial);
        self
    }

    pub fn with_theta(mut self, column: impl Into<Arc<str>>, partial: ThetaSketchPartial) -> Self {
        self.theta.insert(column.into(), partial);
        self
    }

    /// Encode exactly the existing native terminal-report payload. The payload
    /// carries only fragment-local facts and never evidence revision or
    /// provider credentials.
    pub fn to_payload(&self) -> Result<Bytes, StatisticsFragmentError> {
        let mut bytes = Vec::new();
        bytes.push(STATISTICS_FRAGMENT_PAYLOAD_VERSION);
        match &self.table {
            Some(table) => {
                bytes.push(1);
                encode_scalar_partial(&mut bytes, table);
            }
            None => bytes.push(0),
        }
        encode_scalar_partials(&mut bytes, &self.columns)?;
        encode_theta_partials(&mut bytes, &self.theta)?;
        if bytes.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics fragment report exceeds the SPI payload limit",
            ));
        }
        Ok(Bytes::from(bytes))
    }

    pub fn try_from_payload(bytes: &[u8]) -> Result<Self, StatisticsFragmentError> {
        if bytes.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics fragment report exceeds the SPI payload limit",
            ));
        }
        let mut cursor = 0usize;
        let version = take_bytes(bytes, &mut cursor, 1)?[0];
        if version != STATISTICS_FRAGMENT_PAYLOAD_VERSION {
            return Err(StatisticsFragmentError::contract(
                "statistics fragment report has an unsupported version",
            ));
        }
        let table = match take_bytes(bytes, &mut cursor, 1)?[0] {
            0 => None,
            1 => Some(decode_scalar_partial(bytes, &mut cursor)?),
            _ => {
                return Err(StatisticsFragmentError::contract(
                    "statistics fragment report has an invalid table flag",
                ));
            }
        };
        let columns = decode_scalar_partials(bytes, &mut cursor)?;
        let theta = decode_theta_partials(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(StatisticsFragmentError::contract(
                "statistics fragment report has trailing bytes",
            ));
        }
        Ok(Self {
            table,
            columns,
            theta,
        })
    }
}

impl StatisticsScalarPartial {
    fn try_new_bounds(
        row_count: u64,
        null_count: u64,
        total_size: u64,
        minimum: Option<StatisticsScalarBound>,
        maximum: Option<StatisticsScalarBound>,
    ) -> Result<Self, StatisticsFragmentError> {
        if null_count > row_count {
            return Err(StatisticsFragmentError::contract(
                "statistics null count exceeds row count",
            ));
        }
        if minimum.as_ref().is_some_and(|value| !value.is_valid())
            || maximum.as_ref().is_some_and(|value| !value.is_valid())
            || matches!((&minimum, &maximum), (Some(minimum), Some(maximum)) if minimum.compare(maximum).is_none_or(|order| order.is_gt()))
        {
            return Err(StatisticsFragmentError::contract(
                "statistics scalar bounds must be finite, equally typed, and ordered",
            ));
        }
        Ok(Self {
            row_count,
            null_count,
            total_size,
            minimum,
            maximum,
        })
    }
}

impl StatisticsScalarAccumulator {
    fn push(&mut self, array: &ArrayRef, rows: u64) -> Result<(), StatisticsFragmentError> {
        self.row_count = self
            .row_count
            .checked_add(rows)
            .ok_or_else(|| StatisticsFragmentError::exhausted("statistics row count overflow"))?;
        self.null_count = self
            .null_count
            .checked_add(u64::try_from(array.null_count()).map_err(|_| {
                StatisticsFragmentError::exhausted("statistics null count exceeds u64")
            })?)
            .ok_or_else(|| StatisticsFragmentError::exhausted("statistics null count overflow"))?;
        self.total_size = self
            .total_size
            .checked_add(estimated_value_bytes(array)?)
            .ok_or_else(|| StatisticsFragmentError::exhausted("statistics value size overflow"))?;
        for value in array_scalar_bounds(array)? {
            self.minimum = merge_scalar_bounds(self.minimum.take(), Some(value.clone()), true)?;
            self.maximum = merge_scalar_bounds(self.maximum.take(), Some(value), false)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<StatisticsScalarPartial, StatisticsFragmentError> {
        StatisticsScalarPartial::try_new_bounds(
            self.row_count,
            self.null_count,
            self.total_size,
            self.minimum,
            self.maximum,
        )
    }
}

impl StatisticsThetaAccumulator {
    fn new(lg_k: u8) -> Self {
        debug_assert!((5..=12).contains(&lg_k));
        Self {
            sketch: ThetaSketch::builder().lg_k(lg_k).build(),
        }
    }

    fn push(&mut self, array: &ArrayRef) -> Result<(), StatisticsFragmentError> {
        for hash in array_hashes(array)? {
            self.sketch.update(hash as i64);
        }
        Ok(())
    }

    fn finish(self) -> Result<ThetaSketchPartial, StatisticsFragmentError> {
        ThetaSketchPartial::try_from_sketch(self.sketch)
    }
}

impl StatisticsScalarBound {
    fn is_valid(&self) -> bool {
        !matches!(self, Self::F64(value) if !value.is_finite())
    }

    fn compare(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::F64(left), Self::F64(right)) => left.partial_cmp(right),
            (Self::LargeInt(left), Self::LargeInt(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

impl ThetaSketchPartial {
    fn try_from_sketch(mut sketch: ThetaSketch) -> Result<Self, StatisticsFragmentError> {
        sketch.trim();
        let lg_k = sketch.lg_k();
        let mut retained_hashes = sketch.iter().collect::<Vec<_>>();
        retained_hashes.sort_unstable();
        if retained_hashes.len() > MAX_STATISTICS_THETA_RETAINED_HASHES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics Theta partial exceeds the retained-hash limit",
            ));
        }
        Ok(Self {
            lg_k,
            theta: sketch.theta64(),
            retained_hashes,
        })
    }

    fn to_wire_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            THETA_PARTIAL_WIRE_HEADER_BYTES
                + self.retained_hashes.len() * std::mem::size_of::<u64>(),
        );
        bytes.push(THETA_PARTIAL_WIRE_VERSION);
        bytes.push(self.lg_k);
        bytes.extend_from_slice(&self.theta.to_be_bytes());
        bytes.extend_from_slice(&(self.retained_hashes.len() as u32).to_be_bytes());
        for hash in &self.retained_hashes {
            bytes.extend_from_slice(&hash.to_be_bytes());
        }
        bytes
    }

    fn try_from_wire_bytes(bytes: &[u8]) -> Result<Self, StatisticsFragmentError> {
        if bytes.len() < THETA_PARTIAL_WIRE_HEADER_BYTES {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state is truncated",
            ));
        }
        if bytes[0] != THETA_PARTIAL_WIRE_VERSION {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state has an unsupported version",
            ));
        }
        let lg_k = bytes[1];
        if !(5..=12).contains(&lg_k) {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state has an invalid lg_k",
            ));
        }
        let theta = u64::from_be_bytes(bytes[2..10].try_into().expect("slice width checked"));
        let count =
            u32::from_be_bytes(bytes[10..14].try_into().expect("slice width checked")) as usize;
        if count > MAX_STATISTICS_THETA_RETAINED_HASHES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics Theta wire state exceeds the retained-hash limit",
            ));
        }
        let expected = THETA_PARTIAL_WIRE_HEADER_BYTES
            .checked_add(
                count
                    .checked_mul(std::mem::size_of::<u64>())
                    .ok_or_else(|| {
                        StatisticsFragmentError::exhausted(
                            "statistics Theta wire state length overflow",
                        )
                    })?,
            )
            .ok_or_else(|| {
                StatisticsFragmentError::exhausted("statistics Theta wire state length overflow")
            })?;
        if bytes.len() != expected {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state has an invalid length",
            ));
        }
        let retained_hashes = bytes[THETA_PARTIAL_WIRE_HEADER_BYTES..]
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|chunk| u64::from_be_bytes(chunk.try_into().expect("exact chunks")))
            .collect::<Vec<_>>();
        if retained_hashes.windows(2).any(|pair| pair[0] >= pair[1])
            || retained_hashes.iter().any(|hash| *hash >= theta)
        {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state is not canonical",
            ));
        }
        Ok(Self {
            lg_k,
            theta,
            retained_hashes,
        })
    }
}

fn statistics_metric_column(metric: &StatisticsMetric) -> Option<&Arc<str>> {
    match metric {
        StatisticsMetric::RowCount => None,
        StatisticsMetric::NullCount { column }
        | StatisticsMetric::Minimum { column }
        | StatisticsMetric::Maximum { column }
        | StatisticsMetric::AverageSize { column }
        | StatisticsMetric::ThetaNdv { column } => Some(column),
    }
}

fn estimated_value_bytes(array: &ArrayRef) -> Result<u64, StatisticsFragmentError> {
    let bytes = if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        array
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .try_fold(0_u64, |total, value| total.checked_add(value).ok_or(()))
            .map_err(|_| StatisticsFragmentError::exhausted("statistics string size overflow"))?
    } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        array
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .try_fold(0_u64, |total, value| total.checked_add(value).ok_or(()))
            .map_err(|_| StatisticsFragmentError::exhausted("statistics string size overflow"))?
    } else {
        u64::try_from(array.get_array_memory_size())
            .map_err(|_| StatisticsFragmentError::exhausted("statistics value size exceeds u64"))?
    };
    Ok(bytes)
}

fn merge_scalar_bounds(
    left: Option<StatisticsScalarBound>,
    right: Option<StatisticsScalarBound>,
    minimum: bool,
) -> Result<Option<StatisticsScalarBound>, StatisticsFragmentError> {
    match (left, right) {
        (Some(left), Some(right)) => {
            let ordering = left.compare(&right).ok_or_else(|| {
                StatisticsFragmentError::contract(
                    "statistics scalar bounds use incompatible physical types",
                )
            })?;
            Ok(Some(
                if (minimum && ordering.is_gt()) || (!minimum && ordering.is_lt()) {
                    right
                } else {
                    left
                },
            ))
        }
        (value @ Some(_), None) | (None, value @ Some(_)) => Ok(value),
        (None, None) => Ok(None),
    }
}

fn array_scalar_bounds(
    array: &ArrayRef,
) -> Result<Vec<StatisticsScalarBound>, StatisticsFragmentError> {
    macro_rules! f64_values {
        ($array:expr) => {
            return Ok($array
                .iter()
                .flatten()
                .map(|value| StatisticsScalarBound::F64(value as f64))
                .collect())
        };
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        return array
            .iter()
            .flatten()
            .map(|value| {
                let value = value as f64;
                value
                    .is_finite()
                    .then_some(StatisticsScalarBound::F64(value))
                    .ok_or_else(|| {
                        StatisticsFragmentError::contract("statistics numeric value is not finite")
                    })
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return array
            .iter()
            .flatten()
            .map(|value| {
                value
                    .is_finite()
                    .then_some(StatisticsScalarBound::F64(value))
                    .ok_or_else(|| {
                        StatisticsFragmentError::contract("statistics numeric value is not finite")
                    })
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        if array.value_length() != novarocks_types::largeint::LARGEINT_BYTE_WIDTH {
            return Ok(Vec::new());
        }
        return array
            .iter()
            .flatten()
            .map(|value| {
                novarocks_types::largeint::i128_from_be_bytes(value)
                    .map(StatisticsScalarBound::LargeInt)
                    .map_err(|error| {
                        StatisticsFragmentError::contract(format!(
                            "statistics LARGEINT value: {error}"
                        ))
                    })
            })
            .collect();
    }
    Ok(Vec::new())
}

fn array_hashes(array: &ArrayRef) -> Result<Vec<u64>, StatisticsFragmentError> {
    let mut values = Vec::new();
    macro_rules! hash_values {
        ($array:expr) => {{
            for value in $array.iter().flatten() {
                values.push(statistics_value_hash(&value.to_be_bytes()));
            }
            return Ok(values);
        }};
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value.as_bytes()));
        }
        return Ok(values);
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value.as_bytes()));
        }
        return Ok(values);
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value));
        }
        return Ok(values);
    }
    Err(StatisticsFragmentError::contract(
        "statistics Theta collection does not support the requested Arrow type",
    ))
}

fn statistics_value_hash(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 digest has at least eight bytes"),
    )
}

fn take_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    count: usize,
) -> Result<&'a [u8], StatisticsFragmentError> {
    let end = cursor.checked_add(count).ok_or_else(|| {
        StatisticsFragmentError::contract("statistics fragment report length overflow")
    })?;
    let output = bytes.get(*cursor..end).ok_or_else(|| {
        StatisticsFragmentError::contract("statistics fragment report is truncated")
    })?;
    *cursor = end;
    Ok(output)
}

fn encode_scalar_partial(bytes: &mut Vec<u8>, partial: &StatisticsScalarPartial) {
    bytes.extend_from_slice(&partial.row_count.to_be_bytes());
    bytes.extend_from_slice(&partial.null_count.to_be_bytes());
    bytes.extend_from_slice(&partial.total_size.to_be_bytes());
    for value in [&partial.minimum, &partial.maximum] {
        match value {
            Some(StatisticsScalarBound::F64(value)) => {
                bytes.push(1);
                bytes.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            Some(StatisticsScalarBound::LargeInt(value)) => {
                bytes.push(2);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            None => bytes.push(0),
        }
    }
}

fn decode_scalar_partial(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<StatisticsScalarPartial, StatisticsFragmentError> {
    let read_u64 = |cursor: &mut usize| -> Result<u64, StatisticsFragmentError> {
        Ok(u64::from_be_bytes(
            take_bytes(bytes, cursor, 8)?
                .try_into()
                .expect("fixed scalar field width"),
        ))
    };
    let row_count = read_u64(cursor)?;
    let null_count = read_u64(cursor)?;
    let total_size = read_u64(cursor)?;
    let read_bound =
        |cursor: &mut usize| -> Result<Option<StatisticsScalarBound>, StatisticsFragmentError> {
            match take_bytes(bytes, cursor, 1)?[0] {
                0 => Ok(None),
                1 => Ok(Some(StatisticsScalarBound::F64(f64::from_bits(
                    u64::from_be_bytes(
                        take_bytes(bytes, cursor, 8)?
                            .try_into()
                            .expect("fixed scalar field width"),
                    ),
                )))),
                2 => Ok(Some(StatisticsScalarBound::LargeInt(i128::from_be_bytes(
                    take_bytes(bytes, cursor, 16)?
                        .try_into()
                        .expect("fixed LARGEINT scalar field width"),
                )))),
                _ => Err(StatisticsFragmentError::contract(
                    "statistics scalar partial has an invalid bound flag",
                )),
            }
        };
    StatisticsScalarPartial::try_new_bounds(
        row_count,
        null_count,
        total_size,
        read_bound(cursor)?,
        read_bound(cursor)?,
    )
}

fn encode_scalar_partials(
    bytes: &mut Vec<u8>,
    partials: &BTreeMap<Arc<str>, StatisticsScalarPartial>,
) -> Result<(), StatisticsFragmentError> {
    let count = u16::try_from(partials.len()).map_err(|_| {
        StatisticsFragmentError::exhausted("statistics fragment report has too many scalar columns")
    })?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (column, partial) in partials {
        encode_fragment_column(bytes, column)?;
        encode_scalar_partial(bytes, partial);
    }
    Ok(())
}

fn decode_scalar_partials(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<BTreeMap<Arc<str>, StatisticsScalarPartial>, StatisticsFragmentError> {
    let count = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed count width"),
    ) as usize;
    let mut partials = BTreeMap::new();
    for _ in 0..count {
        let column = decode_fragment_column(bytes, cursor)?;
        let value = decode_scalar_partial(bytes, cursor)?;
        if partials.insert(column, value).is_some() {
            return Err(StatisticsFragmentError::contract(
                "statistics fragment report has duplicate scalar columns",
            ));
        }
    }
    Ok(partials)
}

fn encode_theta_partials(
    bytes: &mut Vec<u8>,
    partials: &BTreeMap<Arc<str>, ThetaSketchPartial>,
) -> Result<(), StatisticsFragmentError> {
    let count = u16::try_from(partials.len()).map_err(|_| {
        StatisticsFragmentError::exhausted("statistics fragment report has too many Theta columns")
    })?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (column, partial) in partials {
        encode_fragment_column(bytes, column)?;
        let theta = partial.to_wire_bytes();
        let theta_len = u32::try_from(theta.len()).map_err(|_| {
            StatisticsFragmentError::exhausted("statistics fragment Theta state is too large")
        })?;
        bytes.extend_from_slice(&theta_len.to_be_bytes());
        bytes.extend_from_slice(&theta);
    }
    Ok(())
}

fn decode_theta_partials(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<BTreeMap<Arc<str>, ThetaSketchPartial>, StatisticsFragmentError> {
    let count = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed count width"),
    ) as usize;
    let mut partials = BTreeMap::new();
    for _ in 0..count {
        let column = decode_fragment_column(bytes, cursor)?;
        let theta_len = u32::from_be_bytes(
            take_bytes(bytes, cursor, 4)?
                .try_into()
                .expect("fixed length width"),
        ) as usize;
        let value = ThetaSketchPartial::try_from_wire_bytes(take_bytes(bytes, cursor, theta_len)?)?;
        if partials.insert(column, value).is_some() {
            return Err(StatisticsFragmentError::contract(
                "statistics fragment report has duplicate Theta columns",
            ));
        }
    }
    Ok(partials)
}

fn encode_fragment_column(
    bytes: &mut Vec<u8>,
    column: &Arc<str>,
) -> Result<(), StatisticsFragmentError> {
    let column = column.as_bytes();
    let length = u16::try_from(column.len()).map_err(|_| {
        StatisticsFragmentError::exhausted("statistics fragment report column name is too large")
    })?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(column);
    Ok(())
}

fn decode_fragment_column(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<Arc<str>, StatisticsFragmentError> {
    let length = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed length width"),
    ) as usize;
    let column = std::str::from_utf8(take_bytes(bytes, cursor, length)?).map_err(|_| {
        StatisticsFragmentError::contract("statistics fragment report column is not UTF-8")
    })?;
    if column.is_empty() {
        return Err(StatisticsFragmentError::contract(
            "statistics fragment report has an empty column name",
        ));
    }
    Ok(Arc::from(column))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::{StatisticsMetric, StatisticsMetricRequest};

    use super::{StatisticsBatchCollector, StatisticsFragmentPartial};

    #[test]
    fn fragment_payload_roundtrips_after_arrow_collection() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::NullCount { column: "v".into() },
            StatisticsMetric::ThetaNdv { column: "v".into() },
        ])
        .expect("metrics");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(2),
                Some(2),
            ]))],
        )
        .expect("batch");
        let mut collector = StatisticsBatchCollector::try_new(schema, metrics).expect("collector");
        collector.push_batch(&batch).expect("push batch");
        let payload = collector.finish_fragment_payload().expect("payload");
        let partial =
            StatisticsFragmentPartial::try_from_payload(&payload).expect("decode payload");
        assert_eq!(payload, partial.to_payload().expect("re-encode payload"));
    }

    #[test]
    fn fragment_payload_rejects_trailing_bytes() {
        let error = StatisticsFragmentPartial::try_from_payload(&[2, 0, 0, 0, 0, 0, 7])
            .expect_err("trailing payload must fail");
        assert!(error.to_string().contains("trailing"));
    }
}
