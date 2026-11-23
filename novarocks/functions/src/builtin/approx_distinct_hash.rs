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
use crate::kernel_control::invalid;
use crate::{FunctionValueType, KernelFailure};
use arrow_array::{
    Array, BinaryArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
};
use arrow_schema::DataType;
use novarocks_type_contract::{TemporalType, TemporalValue, ValueLogicalType};
use novarocks_value_arrays::decode::PreparedTemporalArray;
use xxhash_rust::xxh64::{Xxh64, xxh64};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HashProfile {
    Int8,
    Int16,
    Int32,
    Int64,
    Real,
    Double,
    Date,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    ShortDecimal,
    LongDecimal,
    Temporal(TemporalType),
}

impl HashProfile {
    pub(super) fn prepare(value: &FunctionValueType) -> Option<Self> {
        if let Some(temporal) = value.value_type().temporal() {
            return match temporal {
                TemporalType::Timestamp(_) | TemporalType::TimestampWithTimeZone(_) => {
                    Some(Self::Temporal(temporal))
                }
                _ => None,
            };
        }
        if value.logical_type() != ValueLogicalType::Physical {
            return None;
        }
        Some(match value.data_type() {
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::Float32 => Self::Real,
            DataType::Float64 => Self::Double,
            DataType::Date32 => Self::Date,
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::Decimal128(precision, _) if *precision <= 18 => Self::ShortDecimal,
            DataType::Decimal128(..) => Self::LongDecimal,
            _ => return None,
        })
    }

    pub(super) fn transport_profile(self) -> u8 {
        match self {
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int32 => 3,
            Self::Int64 => 4,
            Self::Real => 5,
            Self::Double => 6,
            Self::Date => 7,
            Self::Utf8 | Self::LargeUtf8 => 8,
            Self::Binary | Self::LargeBinary => 9,
            Self::ShortDecimal => 10,
            Self::LongDecimal => 11,
            Self::Temporal(TemporalType::Timestamp(precision)) => 16 + precision.digits(),
            Self::Temporal(TemporalType::TimestampWithTimeZone(precision)) => {
                32 + precision.digits()
            }
            Self::Temporal(_) => unreachable!("prepared only timestamp types"),
        }
    }

    pub(super) fn hash(
        self,
        array: &dyn Array,
        row: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<u64>, KernelFailure> {
        work.step()?;
        if row >= array.len() {
            return Err(invalid(
                "approx_distinct address exceeds its selected array",
            ));
        }
        if array.is_null(row) {
            return Ok(None);
        }
        macro_rules! value {
            ($carrier:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$carrier>()
                    .ok_or_else(|| {
                        invalid("approx_distinct value differs from its prepared hash carrier")
                    })?
                    .value(row)
            };
        }
        let hash = match self {
            Self::Int8 => hash_long(i64::from(value!(Int8Array))),
            Self::Int16 => hash_long(i64::from(value!(Int16Array))),
            Self::Int32 => hash_long(i64::from(value!(Int32Array))),
            Self::Int64 => hash_long(value!(Int64Array)),
            Self::Date => hash_long(i64::from(value!(Date32Array))),
            Self::Real => {
                let value = value!(Float32Array);
                let bits = if value == 0.0 {
                    0
                } else if value.is_nan() {
                    f32::NAN.to_bits()
                } else {
                    value.to_bits()
                };
                hash_long(i64::from(bits as i32))
            }
            Self::Double => {
                let value = value!(Float64Array);
                let bits = if value == 0.0 {
                    0
                } else if value.is_nan() {
                    f64::NAN.to_bits()
                } else {
                    value.to_bits()
                };
                hash_long(bits as i64)
            }
            Self::Utf8 => hash_bytes(value!(StringArray).as_bytes(), work)?,
            Self::LargeUtf8 => hash_bytes(value!(LargeStringArray).as_bytes(), work)?,
            Self::Binary => hash_bytes(value!(BinaryArray), work)?,
            Self::LargeBinary => hash_bytes(value!(LargeBinaryArray), work)?,
            Self::ShortDecimal => {
                let value = i64::try_from(value!(Decimal128Array)).map_err(|_| {
                    invalid("approx_distinct short decimal value exceeds its declared carrier")
                })?;
                hash_long(value)
            }
            Self::LongDecimal => {
                let value = value!(Decimal128Array);
                hash_long((value >> 64) as i64) ^ hash_long(value as i64)
            }
            Self::Temporal(temporal) => {
                let array = PreparedTemporalArray::bind(temporal, array, true).map_err(|_| {
                    invalid("approx_distinct timestamp differs from its prepared carrier")
                })?;
                let value = array.value(row)
                    .map_err(|_| invalid("approx_distinct cannot read its prepared timestamp"))?
                    .ok_or_else(|| invalid("approx_distinct found a hidden top-level null in the timestamp carrier"))?;
                match (temporal, value) {
                    (TemporalType::Timestamp(p), TemporalValue::Timestamp(clock)) => {
                        let hash = hash_long(clock.micros());
                        if p.digits() <= 6 {
                            hash
                        } else {
                            hash ^ hash_long(i64::from(clock.picoseconds_of_microsecond()))
                        }
                    }
                    (
                        TemporalType::TimestampWithTimeZone(p),
                        TemporalValue::TimestampWithTimeZone { instant, .. },
                    ) => {
                        let hash = hash_long(instant.floor_millis());
                        if p.digits() <= 3 {
                            hash
                        } else {
                            let fraction = instant.as_picos().rem_euclid(1_000_000_000) as i64;
                            hash ^ hash_long(fraction)
                        }
                    }
                    _ => {
                        return Err(invalid(
                            "approx_distinct encountered a foreign temporal value",
                        ));
                    }
                }
            }
        };
        Ok(Some(hash))
    }
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
