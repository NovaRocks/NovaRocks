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

//! Pure schema projection for public connector assignments. No Arrow array or
//! runtime capability is admitted here. Provider logical identities (such as
//! UUID) must still be supplied explicitly and checked by the provider owner.

use crate::ConnectorValueType;

pub fn connector_type_for_arrow(data_type: &arrow_schema::DataType) -> Option<ConnectorValueType> {
    use arrow_schema::{DataType, TimeUnit};
    match data_type {
        DataType::Boolean => Some(ConnectorValueType::Boolean),
        DataType::Int8 => Some(ConnectorValueType::TinyInt),
        DataType::Int16 => Some(ConnectorValueType::SmallInt),
        DataType::Int32 => Some(ConnectorValueType::Integer),
        DataType::Int64 => Some(ConnectorValueType::BigInt),
        DataType::Float32 => Some(ConnectorValueType::Real),
        DataType::Float64 => Some(ConnectorValueType::Double),
        DataType::Decimal128(precision, scale) if *precision <= 38 => {
            Some(ConnectorValueType::Decimal {
                precision: *precision,
                scale: *scale,
            })
        }
        DataType::Date32 => Some(ConnectorValueType::Date),
        DataType::Time64(TimeUnit::Microsecond) => Some(ConnectorValueType::TimeMicros),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            Some(ConnectorValueType::TimestampMillis)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Some(ConnectorValueType::TimestampMicros)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(ConnectorValueType::TimestampNanos),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            Some(ConnectorValueType::TimestampTzMicros)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            Some(ConnectorValueType::TimestampTzNanos)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Some(ConnectorValueType::Varchar)
        }
        DataType::Binary | DataType::BinaryView => Some(ConnectorValueType::Varbinary),
        // Preserve the admitted connector predicate carrier mapping. This is
        // not a logical VARIANT/JSON identity: public logical facts and the
        // provider's pure compiler must prove those independently.
        DataType::LargeBinary => Some(ConnectorValueType::NonComparable),
        DataType::FixedSizeBinary(length) if *length >= 0 => Some(ConnectorValueType::Fixed {
            length: *length as u32,
        }),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => Some(ConnectorValueType::NonComparable),
        _ => None,
    }
}

/// Project the connector comparison type from a complete authored engine
/// value. UUID identity comes from its explicit domain, never fixed width.
pub fn connector_type_for_value_type(
    value_type: &novarocks_type_contract::FunctionValueType,
) -> Option<ConnectorValueType> {
    value_type.validate().ok()?;
    if let Some(temporal) = value_type.value_type().temporal() {
        return Some(ConnectorValueType::Temporal(temporal));
    }
    if value_type.logical_type() == novarocks_type_contract::ValueLogicalType::Uuid {
        Some(ConnectorValueType::Uuid)
    } else {
        connector_type_for_arrow(value_type.data_type())
    }
}

/// Check both the exact carrier and the declared UUID correspondence.
pub fn connector_type_accepts_value_type(
    connector_type: ConnectorValueType,
    value_type: &novarocks_type_contract::FunctionValueType,
) -> bool {
    connector_type_for_value_type(value_type) == Some(connector_type)
}

/// Check an explicitly frozen connector type against its exact Arrow carrier.
/// UUID and fixed binary share a carrier; this check never infers a UUID.
pub fn connector_type_accepts_arrow(
    connector_type: ConnectorValueType,
    data_type: &arrow_schema::DataType,
) -> bool {
    if let ConnectorValueType::Temporal(temporal) = connector_type {
        return temporal.carrier() == *data_type;
    }
    connector_type_for_arrow(data_type) == Some(connector_type)
        || (connector_type == ConnectorValueType::Uuid
            && *data_type == arrow_schema::DataType::FixedSizeBinary(16))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};

    #[test]
    fn complete_assignment_type_preserves_explicit_uuid_identity() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
        let uuid = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap();
        assert_eq!(
            connector_type_for_value_type(&physical),
            Some(ConnectorValueType::Fixed { length: 16 })
        );
        assert_eq!(
            connector_type_for_value_type(&uuid),
            Some(ConnectorValueType::Uuid)
        );
        assert!(!connector_type_accepts_value_type(
            ConnectorValueType::Uuid,
            &physical
        ));
        assert!(!connector_type_accepts_value_type(
            ConnectorValueType::Fixed { length: 16 },
            &uuid
        ));
        assert!(FunctionValueType::try_with_logical_type(
            DataType::Utf8, false, ValueLogicalType::Uuid,
        ).is_err(), "an invalid UUID binding is refused by its actual author");
    }

    #[test]
    fn assignment_types_preserve_exact_width_scale_and_time_unit() {
        assert!(connector_type_accepts_arrow(
            ConnectorValueType::BigInt,
            &DataType::Int64
        ));
        assert!(!connector_type_accepts_arrow(
            ConnectorValueType::Varchar,
            &DataType::Int64
        ));
        assert!(!connector_type_accepts_arrow(
            ConnectorValueType::Integer,
            &DataType::Int64
        ));
        assert!(!connector_type_accepts_arrow(
            ConnectorValueType::Decimal {
                precision: 18,
                scale: 2
            },
            &DataType::Decimal128(18, 3)
        ));
        assert!(!connector_type_accepts_arrow(
            ConnectorValueType::TimestampMicros,
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        ));
        assert!(!connector_type_accepts_arrow(
            ConnectorValueType::TimestampMicros,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ));
        assert_eq!(connector_type_for_arrow(&DataType::UInt64), None);
    }

    #[test]
    fn uuid_identity_is_not_inferred_from_fixed_binary() {
        assert_eq!(
            connector_type_for_arrow(&DataType::FixedSizeBinary(16)),
            Some(ConnectorValueType::Fixed { length: 16 })
        );
        assert!(connector_type_accepts_arrow(
            ConnectorValueType::Uuid,
            &DataType::FixedSizeBinary(16)
        ));
        assert!(!connector_type_accepts_arrow(
            ConnectorValueType::Uuid,
            &DataType::FixedSizeBinary(15)
        ));
        assert_eq!(
            connector_type_for_arrow(&DataType::LargeBinary),
            Some(ConnectorValueType::NonComparable)
        );
    }
}
