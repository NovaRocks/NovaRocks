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

use crate::aggregate_scalar::ScalarWork;
use crate::json_value::{
    Container as JsonContainer, Entries as JsonEntries, Kind as JsonKind, Node as JsonNode,
    Number as JsonNumber, Scalar as JsonScalar,
};
use crate::kernel_control::{internal, invalid};
use crate::{FunctionValueType, KernelFailure};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CharacterKind, PICOS_PER_DAY, PICOS_PER_MINUTE, TemporalType, TemporalValue, ValueKind,
    ValueLogicalType, ValueTypeRef,
};
use novarocks_value_arrays::decode::PreparedTemporalArray;
use std::sync::Arc;
use xxhash_rust::xxh64::{Xxh64, xxh64};

// Trino uses separate numerical HASH_CODE and XX_HASH_64 operators. TypeOperators
// uses XX_HASH_64 for a HASH_CODE request only when a type has no HASH_CODE.
#[derive(Clone, Copy, Debug)]
enum HashOperation {
    XxHash64,
    HashCode,
}

impl HashOperation {
    fn long(self, value: i64) -> u64 {
        match self {
            Self::XxHash64 => hash_long(value),
            Self::HashCode => (value as u64)
                .wrapping_mul(0xc2b2_ae3d_27d4_eb4f)
                .rotate_left(31)
                .wrapping_mul(0x9e37_79b1_85eb_ca87),
        }
    }
}

#[derive(Debug)]
struct HashRecipe {
    operation: HashOperation,
    kind: HashKind,
}

#[derive(Debug)]
enum HashKind {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Real,
    Double,
    Date,
    Text {
        trim_padding: bool,
    },
    Binary,
    Decimal {
        short: bool,
    },
    Temporal(TemporalType),
    Uuid,
    Json,
    List(Box<HashRecipe>),
    Row(Box<[HashRecipe]>),
    Map {
        key: Box<HashRecipe>,
        value: Box<HashRecipe>,
    },
}

// The immutable recipe supplies both the actual operator and the exact recipe
// bytes written in a partial state. Full call provenance is checked separately
// against the exact aggregate signature.
#[derive(Debug)]
struct PreparedHash {
    recipe: HashRecipe,
    transport: Box<[u8]>,
}

#[derive(Clone, Debug)]
pub(crate) struct HashProfile(Arc<PreparedHash>);

impl PartialEq for HashProfile {
    fn eq(&self, other: &Self) -> bool {
        self.transport_profile() == other.transport_profile()
    }
}

impl Eq for HashProfile {}

impl HashProfile {
    pub(super) fn prepare(value: &FunctionValueType) -> Option<Self> {
        let mut transport = Vec::new();
        let recipe = HashRecipe::prepare(
            value.value_type(),
            value.data_type(),
            HashOperation::XxHash64,
            &mut transport,
        )?;
        Some(Self(Arc::new(PreparedHash {
            recipe,
            transport: transport.into_boxed_slice(),
        })))
    }

    pub(super) fn transport_profile(&self) -> &[u8] {
        &self.0.transport
    }

    pub(super) fn hash(
        &self,
        array: &dyn Array,
        row: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<u64>, KernelFailure> {
        self.0.recipe.hash(array, row, work)
    }
}

impl HashRecipe {
    fn prepare(
        value: &ValueTypeRef,
        layout: &DataType,
        operation: HashOperation,
        transport: &mut Vec<u8>,
    ) -> Option<Self> {
        use HashKind as Kind;
        // Each record starts with its requested operator and domain. Containers
        // have fixed arity except for ROW, whose field count precedes its children.
        transport.push(match operation {
            HashOperation::XxHash64 => 0,
            HashOperation::HashCode => 1,
        });
        let kind = match (value.kind(), layout) {
            (ValueKind::Scalar(ty), _) if ty == layout => {
                let (tag, kind) = match ty {
                    DataType::Null => (0, Kind::Null),
                    DataType::Boolean => (1, Kind::Boolean),
                    DataType::Int8 => (2, Kind::Int8),
                    DataType::Int16 => (3, Kind::Int16),
                    DataType::Int32 => (4, Kind::Int32),
                    DataType::Int64 => (5, Kind::Int64),
                    DataType::Float32 => (6, Kind::Real),
                    DataType::Float64 => (7, Kind::Double),
                    DataType::Date32 => (8, Kind::Date),
                    DataType::Decimal128(precision, _) if *precision <= 18 => {
                        (9, Kind::Decimal { short: true })
                    }
                    DataType::Decimal128(..) => (10, Kind::Decimal { short: false }),
                    _ => return None,
                };
                transport.push(tag);
                kind
            }
            (
                ValueKind::String | ValueKind::BoundedCharacter(_),
                DataType::Utf8 | DataType::LargeUtf8,
            ) => {
                let trim_padding = matches!(
                    value.kind(),
                    ValueKind::BoundedCharacter(ty) if ty.kind() == CharacterKind::Char
                );
                transport.push(if trim_padding { 12 } else { 11 });
                Kind::Text { trim_padding }
            }
            (ValueKind::Binary, DataType::Binary | DataType::LargeBinary) => {
                transport.push(13);
                Kind::Binary
            }
            (ValueKind::Temporal(temporal), _) if layout == &temporal.carrier() => {
                let precision = temporal.precision().digits();
                let domain = match temporal {
                    TemporalType::Time(_) => 0,
                    TemporalType::TimeWithTimeZone(_) => 1,
                    TemporalType::Timestamp(_) => 2,
                    TemporalType::TimestampWithTimeZone(_) => 3,
                    TemporalType::Interval(q) if q.is_year_month() => 4,
                    TemporalType::Interval(_) => 5,
                };
                transport.extend_from_slice(&[14, domain, precision]);
                Kind::Temporal(*temporal)
            }
            (ValueKind::Nominal(ValueLogicalType::Uuid), DataType::FixedSizeBinary(16)) => {
                transport.push(15);
                Kind::Uuid
            }
            (ValueKind::Nominal(ValueLogicalType::Json), DataType::LargeBinary) => {
                transport.push(16);
                Kind::Json
            }
            (ValueKind::List(item), DataType::List(field) | DataType::LargeList(field)) => {
                transport.push(17);
                let item = Self::prepare(item, field.data_type(), operation, transport)?;
                Kind::List(Box::new(item))
            }
            (ValueKind::Struct(fields), DataType::Struct(layout_fields))
                if fields.len() == layout_fields.len() =>
            {
                let columns = u16::try_from(fields.len()).ok()?;
                transport.push(18);
                transport.extend_from_slice(&columns.to_le_bytes());
                let fields = fields
                    .iter()
                    .zip(layout_fields.iter())
                    .map(|(field, layout_field)| {
                        Self::prepare(
                            field.value_type(),
                            layout_field.data_type(),
                            HashOperation::HashCode,
                            transport,
                        )
                    })
                    .collect::<Option<Box<[_]>>>()?;
                Kind::Row(fields)
            }
            (ValueKind::Map { key, value }, DataType::Map(layout_entries, _)) => {
                let DataType::Struct(fields) = layout_entries.data_type() else {
                    return None;
                };
                let [key_field, value_field] = &fields[..] else {
                    return None;
                };
                transport.push(19);
                let key = Self::prepare(key, key_field.data_type(), operation, transport)?;
                let value = Self::prepare(value, value_field.data_type(), operation, transport)?;
                Kind::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                }
            }
            // VARIANT is outside the installed hash domain until the public
            // logical type has both its reference operator and a NovaRocks
            // recipe over the admitted values.
            _ => return None,
        };
        Some(Self { operation, kind })
    }

    fn hash(
        &self,
        array: &dyn Array,
        row: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<u64>, KernelFailure> {
        use HashKind as Kind;
        work.step()?;
        if row >= array.len() {
            return Err(invalid(
                "approx_distinct address exceeds its selected array",
            ));
        }
        // Arrow's NullArray has no validity bitmap even though every logical
        // cell is NULL.
        if matches!(self.kind, Kind::Null) && array.data_type() == &DataType::Null {
            return Ok(None);
        }
        if array.is_null(row) {
            return Ok(None);
        }
        macro_rules! value {
            ($carrier:ty) => {
                carrier::<$carrier>(array)?.value(row)
            };
        }
        let hash = match &self.kind {
            Kind::Null => return Err(invalid("approx_distinct encountered a non-NULL UNKNOWN")),
            Kind::Boolean => hash_long(i64::from(value!(BooleanArray))),
            Kind::Int8 => self.operation.long(i64::from(value!(Int8Array))),
            Kind::Int16 => self.operation.long(i64::from(value!(Int16Array))),
            Kind::Int32 => self.operation.long(i64::from(value!(Int32Array))),
            Kind::Int64 => self.operation.long(value!(Int64Array)),
            Kind::Date => self.operation.long(i64::from(value!(Date32Array))),
            Kind::Real => {
                let value = value!(Float32Array);
                let bits = if value == 0.0 {
                    0
                } else if value.is_nan() {
                    f32::NAN.to_bits()
                } else {
                    value.to_bits()
                };
                self.operation.long(i64::from(bits as i32))
            }
            Kind::Double => {
                let value = value!(Float64Array);
                let bits = if value == 0.0 {
                    0
                } else if value.is_nan() {
                    f64::NAN.to_bits()
                } else {
                    value.to_bits()
                };
                self.operation.long(bits as i64)
            }
            Kind::Text { trim_padding } => {
                let text = match array.data_type() {
                    DataType::Utf8 => value!(StringArray),
                    DataType::LargeUtf8 => value!(LargeStringArray),
                    _ => return Err(wrong_carrier()),
                };
                let text = if *trim_padding {
                    text.trim_end_matches(' ')
                } else {
                    text
                };
                hash_bytes(text.as_bytes(), work)?
            }
            Kind::Binary => {
                let bytes = match array.data_type() {
                    DataType::Binary => value!(BinaryArray),
                    DataType::LargeBinary => value!(LargeBinaryArray),
                    _ => return Err(wrong_carrier()),
                };
                hash_bytes(bytes, work)?
            }
            Kind::Decimal { short: true } => {
                let value = i64::try_from(value!(Decimal128Array)).map_err(|_| {
                    invalid("approx_distinct short decimal value exceeds its declared carrier")
                })?;
                self.operation.long(value)
            }
            Kind::Decimal { short: false } => {
                let value = value!(Decimal128Array);
                hash_long((value >> 64) as i64) ^ hash_long(value as i64)
            }
            Kind::Temporal(temporal) => temporal_hash(self.operation, *temporal, array, row)?,
            Kind::Uuid => {
                let bytes: &[u8; 16] = value!(FixedSizeBinaryArray)
                    .try_into()
                    .map_err(|_| wrong_carrier())?;
                let (first, last) = bytes.split_at(8);
                xxh64(first, 0) ^ xxh64(last, 0)
            }
            Kind::Json => {
                let node =
                    JsonNode::from_root(value!(LargeBinaryArray)).map_err(|_| malformed_json())?;
                json_hash(node, work)?
            }
            Kind::List(item) => match array.data_type() {
                DataType::List(_) => {
                    let list = carrier::<ListArray>(array)?;
                    let offsets = list.value_offsets();
                    item.list_hash(
                        list.values().as_ref(),
                        offsets[row] as usize..offsets[row + 1] as usize,
                        work,
                    )?
                }
                DataType::LargeList(_) => {
                    let list = carrier::<LargeListArray>(array)?;
                    let offsets = list.value_offsets();
                    item.list_hash(
                        list.values().as_ref(),
                        offsets[row] as usize..offsets[row + 1] as usize,
                        work,
                    )?
                }
                _ => return Err(wrong_carrier()),
            },
            Kind::Row(fields) => {
                let row_array = carrier::<StructArray>(array)?;
                if row_array.num_columns() != fields.len() {
                    return Err(wrong_carrier());
                }
                let mut hash = 1_u64;
                for (field, column) in fields.iter().zip(row_array.columns()) {
                    hash = fold_ordered(hash, field.hash(column.as_ref(), row, work)?.unwrap_or(0));
                }
                hash
            }
            Kind::Map { key, value } => {
                let map = carrier::<MapArray>(array)?;
                let offsets = map.value_offsets();
                let mut hash = 0_u64;
                for entry in offsets[row] as usize..offsets[row + 1] as usize {
                    let key_hash = key.hash(map.keys().as_ref(), entry, work)?.unwrap_or(0);
                    let value_hash = value.hash(map.values().as_ref(), entry, work)?.unwrap_or(0);
                    hash = hash.wrapping_add(key_hash ^ value_hash);
                }
                hash
            }
        };
        Ok(Some(hash))
    }

    fn list_hash(
        &self,
        values: &dyn Array,
        rows: std::ops::Range<usize>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<u64, KernelFailure> {
        let mut hash = 0;
        for row in rows {
            hash = fold_ordered(hash, self.hash(values, row, work)?.unwrap_or(0));
        }
        Ok(hash)
    }
}

fn temporal_hash(
    operation: HashOperation,
    temporal: TemporalType,
    array: &dyn Array,
    row: usize,
) -> Result<u64, KernelFailure> {
    let array = PreparedTemporalArray::bind(temporal, array, true).map_err(|_| wrong_carrier())?;
    let value = array
        .value(row)
        .map_err(|_| invalid("approx_distinct cannot read its prepared temporal value"))?
        .ok_or_else(|| {
            invalid("approx_distinct found a hidden top-level null in the temporal carrier")
        })?;
    let precision = temporal.precision().digits();
    Ok(match (temporal, value) {
        (TemporalType::Time(_), TemporalValue::Time(picos)) => operation.long(picos),
        (
            TemporalType::TimeWithTimeZone(_),
            TemporalValue::TimeWithTimeZone {
                picos,
                offset_minutes,
            },
        ) => {
            let utc = normalized_time(picos, offset_minutes);
            // The reference short TIME WITH TIME ZONE stores nanoseconds.
            operation.long(if precision <= 9 { utc / 1_000 } else { utc })
        }
        (TemporalType::Timestamp(_), TemporalValue::Timestamp(clock))
        | (TemporalType::Interval(_), TemporalValue::IntervalDayTime(clock)) => {
            if precision <= 6 {
                operation.long(clock.micros())
            } else {
                hash_long(clock.micros()) ^ hash_long(i64::from(clock.picoseconds_of_microsecond()))
            }
        }
        (
            TemporalType::TimestampWithTimeZone(_),
            TemporalValue::TimestampWithTimeZone { instant, .. },
        ) => {
            if precision <= 3 {
                operation.long(instant.floor_millis())
            } else {
                hash_long(instant.floor_millis())
                    ^ hash_long(instant.as_picos().rem_euclid(1_000_000_000) as i64)
            }
        }
        (TemporalType::Interval(_), TemporalValue::IntervalYearMonth(months)) => {
            operation.long(i64::from(months))
        }
        _ => {
            return Err(invalid(
                "approx_distinct encountered a foreign temporal value",
            ));
        }
    })
}

fn normalized_time(picos: i64, offset_minutes: i16) -> i64 {
    (picos - i64::from(offset_minutes) * PICOS_PER_MINUTE).rem_euclid(PICOS_PER_DAY)
}

fn fold_ordered(hash: u64, child: u64) -> u64 {
    hash.wrapping_mul(31).wrapping_add(child)
}

fn carrier<T: Array + 'static>(array: &dyn Array) -> Result<&T, KernelFailure> {
    array.as_any().downcast_ref::<T>().ok_or_else(wrong_carrier)
}

fn wrong_carrier() -> KernelFailure {
    invalid("approx_distinct value differs from its prepared hash carrier")
}

fn hash_long(value: i64) -> u64 {
    xxh64(&value.to_le_bytes(), 0)
}

fn hash_bytes(bytes: &[u8], work: &mut ScalarWork<'_, '_>) -> Result<u64, KernelFailure> {
    let mut hash = Xxh64::new(0);
    for chunk in bytes.chunks(256) {
        work.step()?;
        hash.update(chunk);
    }
    Ok(hash.digest())
}

fn malformed_json() -> KernelFailure {
    internal("approx_distinct encountered malformed native JSON")
}

fn observed_json_kind<'a>(
    node: JsonNode<'a>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<JsonKind<'a>, KernelFailure> {
    use crate::json_value::ObservedError;
    node.kind_observed(|| work.step())
        .map_err(|error| match error {
            ObservedError::Control(error) => error,
            ObservedError::Value(_) => malformed_json(),
        })
}

fn json_hash(node: JsonNode<'_>, work: &mut ScalarWork<'_, '_>) -> Result<u64, KernelFailure> {
    match observed_json_kind(node, work)? {
        JsonKind::Null => Ok(json_null_hash()),
        JsonKind::Scalar(scalar) => json_scalar_hash(scalar, work),
        JsonKind::Array(array) => json_container_hash(JsonHashFrame::new(array, false), work),
        JsonKind::Object(object) => json_container_hash(JsonHashFrame::new(object, true), work),
    }
}

fn json_null_hash() -> u64 {
    // Trino hashes its canonical binary JSON-null representation.
    xxh64(&[0xf3, 2], 0)
}

struct JsonHashFrame<'a> {
    entries: JsonEntries<'a>,
    object: bool,
    hash: u64,
    member_seed: Option<u64>,
}

impl<'a> JsonHashFrame<'a> {
    fn new(container: JsonContainer<'a>, object: bool) -> Self {
        Self {
            entries: container.entries(),
            object,
            hash: u64::from(!object),
            member_seed: None,
        }
    }

    fn accept_child(&mut self, child: u64) {
        self.hash = match self.member_seed.take() {
            Some(seed) => self.hash.wrapping_add(xxh64(&child.to_le_bytes(), seed)),
            None => fold_ordered(self.hash, child),
        };
    }
}

fn json_container_hash(
    root: JsonHashFrame<'_>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<u64, KernelFailure> {
    // Follow the native JSON decoder's container depth without a recursive
    // Rust call per nested element or per-value heap scratch.
    let mut frames = [const { None }; crate::json_value::MAX_DEPTH];
    frames[0] = Some(root);
    let mut depth = 1;
    let mut child_hash = None;
    loop {
        let frame = frames[depth - 1].as_mut().expect("current JSON container");
        if let Some(child) = child_hash.take() {
            frame.accept_child(child);
        }
        let Some(entry) = frame.entries.next() else {
            let result = frame.hash;
            depth -= 1;
            if depth == 0 {
                return Ok(result);
            }
            child_hash = Some(result);
            continue;
        };
        let entry = entry.map_err(|_| malformed_json())?;
        if frame.object {
            let name = entry.key.ok_or_else(malformed_json)?;
            frame.member_seed = Some(hash_bytes(name.as_bytes(), work)?);
        }
        let nested = match observed_json_kind(entry.value, work)? {
            JsonKind::Null => {
                child_hash = Some(json_null_hash());
                continue;
            }
            JsonKind::Scalar(scalar) => {
                child_hash = Some(json_scalar_hash(scalar, work)?);
                continue;
            }
            JsonKind::Array(array) => JsonHashFrame::new(array, false),
            JsonKind::Object(object) => JsonHashFrame::new(object, true),
        };
        let slot = frames.get_mut(depth).ok_or_else(malformed_json)?;
        *slot = Some(nested);
        depth += 1;
    }
}

fn json_scalar_hash(
    scalar: JsonScalar<'_>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<u64, KernelFailure> {
    let hash = match scalar {
        JsonScalar::Boolean(value) => {
            if value {
                1231
            } else {
                1237
            }
        }
        JsonScalar::String(value) => hash_bytes(value.as_bytes(), work)?,
        JsonScalar::TinyInt(value) => decimal_hash(i128::from(value), 0),
        JsonScalar::SmallInt(value) => decimal_hash(i128::from(value), 0),
        JsonScalar::Integer(value) => decimal_hash(i128::from(value), 0),
        JsonScalar::BigInt(value) => decimal_hash(i128::from(value), 0),
        JsonScalar::Real(value) => float_decimal_hash(f64::from(value)),
        JsonScalar::Double(value) => float_decimal_hash(value),
        JsonScalar::Decimal {
            coefficient, scale, ..
        } => decimal_hash(coefficient, i32::from(scale)),
        JsonScalar::Number(JsonNumber::Finite { coefficient, scale }) => {
            normalized_decimal_hash(coefficient, scale)
        }
        JsonScalar::Number(JsonNumber::NaN) => 0x7ff8_0000_0000_0000,
        JsonScalar::Number(JsonNumber::PositiveInfinity) => f64::INFINITY.to_bits(),
        JsonScalar::Number(JsonNumber::NegativeInfinity) => f64::NEG_INFINITY.to_bits(),
        JsonScalar::Date(value) => json_datetime_hash(1, i64::from(value), 0),
        JsonScalar::Temporal { value, .. } => match value {
            TemporalValue::Time(picos) => json_datetime_hash(2, picos, 0),
            TemporalValue::TimeWithTimeZone {
                picos,
                offset_minutes,
            } => json_datetime_hash(3, normalized_time(picos, offset_minutes), 0),
            TemporalValue::Timestamp(value) => json_datetime_hash(
                4,
                value.micros(),
                i64::from(value.picoseconds_of_microsecond()),
            ),
            TemporalValue::TimestampWithTimeZone { instant, .. } => json_datetime_hash(
                5,
                instant.floor_millis(),
                instant.as_picos().rem_euclid(1_000_000_000) as i64,
            ),
            TemporalValue::IntervalYearMonth(_) | TemporalValue::IntervalDayTime(_) => {
                return Err(malformed_json());
            }
        },
    };
    Ok(hash)
}

fn json_datetime_hash(kind: i64, value: i64, fraction: i64) -> u64 {
    fn java_long_hash(value: i64) -> i64 {
        i64::from(((value as u64) ^ ((value as u64) >> 32)) as i32)
    }
    (31 * (31 * kind + java_long_hash(value)) + java_long_hash(fraction)) as u64
}

fn float_decimal_hash(value: f64) -> u64 {
    match crate::json_value::java_double_decimal(value) {
        Some((coefficient, scale)) => decimal_hash(i128::from(coefficient), scale),
        None if value.is_nan() => 0x7ff8_0000_0000_0000,
        None => value.to_bits(),
    }
}

// Normalize typed JSON scalars with the same decimal equivalence as F6's
// canonical comparison key. NUMBER leaves already have normalized components.
fn decimal_hash(mut coefficient: i128, mut scale: i32) -> u64 {
    if coefficient == 0 {
        return 0;
    }
    while coefficient % 10 == 0 {
        coefficient /= 10;
        scale -= 1;
    }
    normalized_decimal_hash(&coefficient.to_le_bytes(), scale)
}

// Java BigDecimal.hashCode combines scale with the signed BigInteger word
// hash. The native coefficient is little-endian two's complement; reading
// magnitude words from low to high avoids allocating a reversed copy.
fn normalized_decimal_hash(coefficient: &[u8], scale: i32) -> u64 {
    let negative = coefficient.last().is_some_and(|byte| byte & 128 != 0);
    let mut carry = u32::from(negative);
    let mut factor = 1_u32;
    let mut hash = 0_u32;
    for bytes in coefficient.chunks(4) {
        let mut word = [if negative { 255 } else { 0 }; 4];
        word[..bytes.len()].copy_from_slice(bytes);
        let word = u32::from_le_bytes(word);
        let magnitude = if negative {
            let (value, overflow) = (!word).overflowing_add(carry);
            carry = u32::from(overflow);
            value
        } else {
            word
        };
        hash = hash.wrapping_add(magnitude.wrapping_mul(factor));
        factor = factor.wrapping_mul(31);
    }
    if negative {
        hash = hash.wrapping_neg();
    }
    let hash = (hash as i32).wrapping_mul(31).wrapping_add(scale);
    i64::from(hash) as u64
}

#[cfg(test)]
#[path = "approx_distinct_hash_tests.rs"]
mod tests;
