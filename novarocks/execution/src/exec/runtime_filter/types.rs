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

// Staged B3 runtime-filter type model; production consumers land in follow-up tasks.
#![allow(dead_code)]

use arrow::datatypes::{DataType, TimeUnit};

use novarocks_types::largeint;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeFilterType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    LargeInt,
    Float32,
    Float64,
    Date32,
    TimestampMicros,
    TimeMicros,
    Utf8,
    Decimal {
        width: RuntimeDecimalWidth,
        precision: Option<u8>,
        scale: Option<i8>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeDecimalWidth {
    Decimal32,
    Decimal64,
    Decimal128,
}

impl RuntimeFilterType {
    pub fn from_arrow_data_type(data_type: &DataType) -> Result<Self, String> {
        let ty = match data_type {
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                Self::LargeInt
            }
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Date32 => Self::Date32,
            DataType::Timestamp(TimeUnit::Second, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _) => Self::TimestampMicros,
            DataType::Utf8 => Self::Utf8,
            DataType::Decimal128(precision, scale) => Self::Decimal {
                width: RuntimeDecimalWidth::for_precision(*precision)?,
                precision: Some(*precision),
                scale: Some(*scale),
            },
            _ => {
                return Err(format!(
                    "unsupported runtime filter data type: {:?}",
                    data_type
                ));
            }
        };
        Ok(ty)
    }

    pub fn is_utf8_like(self) -> bool {
        matches!(self, Self::Utf8)
    }

    pub fn is_decimal(self) -> bool {
        matches!(self, Self::Decimal { .. })
    }

    pub fn decimal_width(self) -> Option<RuntimeDecimalWidth> {
        match self {
            Self::Decimal { width, .. } => Some(width),
            _ => None,
        }
    }
}

impl RuntimeDecimalWidth {
    pub fn for_precision(precision: u8) -> Result<Self, String> {
        match precision {
            1..=9 => Ok(Self::Decimal32),
            10..=18 => Ok(Self::Decimal64),
            19..=38 => Ok(Self::Decimal128),
            _ => Err(format!(
                "unsupported runtime filter decimal precision: {}",
                precision
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{RuntimeDecimalWidth, RuntimeFilterType};

    #[test]
    fn maps_arrow_scalars_to_runtime_filter_types() {
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::Boolean).unwrap(),
            RuntimeFilterType::Boolean
        );
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::Int8).unwrap(),
            RuntimeFilterType::Int8
        );
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::Int64).unwrap(),
            RuntimeFilterType::Int64
        );
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::FixedSizeBinary(16)).unwrap(),
            RuntimeFilterType::LargeInt
        );
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                None
            ))
            .unwrap(),
            RuntimeFilterType::TimestampMicros
        );
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::Utf8).unwrap(),
            RuntimeFilterType::Utf8
        );
    }

    #[test]
    fn maps_decimal_precision_to_runtime_decimal_width() {
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::Decimal128(9, 0)).unwrap(),
            RuntimeFilterType::Decimal {
                width: RuntimeDecimalWidth::Decimal32,
                precision: Some(9),
                scale: Some(0),
            }
        );
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::Decimal128(18, 2)).unwrap(),
            RuntimeFilterType::Decimal {
                width: RuntimeDecimalWidth::Decimal64,
                precision: Some(18),
                scale: Some(2),
            }
        );
        assert_eq!(
            RuntimeFilterType::from_arrow_data_type(&DataType::Decimal128(38, 6)).unwrap(),
            RuntimeFilterType::Decimal {
                width: RuntimeDecimalWidth::Decimal128,
                precision: Some(38),
                scale: Some(6),
            }
        );
    }

    #[test]
    fn rejects_decimal_precision_boundaries_outside_runtime_widths() {
        let err = RuntimeFilterType::from_arrow_data_type(&DataType::Decimal128(0, 0)).unwrap_err();
        assert!(err.contains("unsupported runtime filter decimal precision: 0"));

        let err =
            RuntimeFilterType::from_arrow_data_type(&DataType::Decimal128(39, 0)).unwrap_err();
        assert!(err.contains("unsupported runtime filter decimal precision: 39"));
    }

    #[test]
    fn rejects_fixed_size_binary_that_is_not_largeint() {
        let err =
            RuntimeFilterType::from_arrow_data_type(&DataType::FixedSizeBinary(15)).unwrap_err();
        assert!(err.contains("unsupported runtime filter data type"));
    }

    #[test]
    fn rejects_time64_until_runtime_filters_support_time_arrays() {
        let err = RuntimeFilterType::from_arrow_data_type(&DataType::Time64(TimeUnit::Microsecond))
            .unwrap_err();
        assert!(err.contains("unsupported runtime filter data type"));

        let err = RuntimeFilterType::from_arrow_data_type(&DataType::Time64(TimeUnit::Nanosecond))
            .unwrap_err();
        assert!(err.contains("unsupported runtime filter data type"));
    }

    #[test]
    fn rejects_unsupported_arrow_types_without_defaulting() {
        let err = RuntimeFilterType::from_arrow_data_type(&DataType::List(std::sync::Arc::new(
            arrow::datatypes::Field::new("item", DataType::Int32, true),
        )))
        .unwrap_err();
        assert!(err.contains("unsupported runtime filter data type"));
    }
}
