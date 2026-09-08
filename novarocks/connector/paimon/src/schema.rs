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

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

pub const MAX_PAIMON_DECIMAL_PRECISION: u8 = 38;
pub const MAX_PAIMON_TIMESTAMP_PRECISION: u8 = 6;

/// Exact supported PAI-1 scalar type. Paimon logical types outside this enum
/// are rejected while the schema is frozen.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PaimonDataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal128 { precision: u8, scale: u8 },
    Utf8,
    Binary,
    Date32,
    TimestampMillis { precision: u8 },
    TimestampMicros { precision: u8 },
}

impl PaimonDataType {
    pub fn decimal(precision: u8, scale: u8) -> Result<Self, ConnectorError> {
        if precision == 0 || precision > MAX_PAIMON_DECIMAL_PRECISION || scale > precision {
            return Err(invalid("Paimon decimal precision or scale is unsupported"));
        }
        Ok(Self::Decimal128 { precision, scale })
    }

    pub fn timestamp(precision: u8) -> Result<Self, ConnectorError> {
        if precision > MAX_PAIMON_TIMESTAMP_PRECISION {
            return Err(invalid("Paimon timestamp precision exceeds microseconds"));
        }
        if precision <= 3 {
            Ok(Self::TimestampMillis { precision })
        } else {
            Ok(Self::TimestampMicros { precision })
        }
    }

    pub const fn supported_as_key(self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Decimal128 { .. }
                | Self::Utf8
                | Self::Date32
        )
    }

    pub const fn is_signed_integer(self) -> bool {
        matches!(self, Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64)
    }
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_and_timestamp_bounds_are_exact() {
        assert_eq!(
            PaimonDataType::decimal(38, 38).unwrap(),
            PaimonDataType::Decimal128 {
                precision: 38,
                scale: 38
            }
        );
        assert!(PaimonDataType::decimal(39, 0).is_err());
        assert!(PaimonDataType::decimal(10, 11).is_err());
        assert_eq!(
            PaimonDataType::timestamp(3).unwrap(),
            PaimonDataType::TimestampMillis { precision: 3 }
        );
        assert_eq!(
            PaimonDataType::timestamp(6).unwrap(),
            PaimonDataType::TimestampMicros { precision: 6 }
        );
        assert!(PaimonDataType::timestamp(7).is_err());
    }
}
