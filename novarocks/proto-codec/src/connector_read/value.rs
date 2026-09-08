//! Typed value conversion between the generated DTOs and the SPI vocabulary.
//!
//! Every value carries its exact type. Decimal precision/scale and fixed width
//! are part of the type, so a mismatch is a typed rejection rather than an
//! implicit widening.

use std::sync::Arc;

use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{ConnectorValue, ConnectorValueType};

use crate::FieldPath;

use super::{
    MAX_SCALAR_BYTES, bounded_bytes, bounded_text, exact_bytes, inconsistent, invalid,
    invalid_enum, missing, out_of_range,
};

const MAX_DECIMAL_PRECISION: u32 = 38;

/// Decode a generated value type, enforcing that its parameters are present
/// exactly for the kinds that require them.
pub fn decode_value_type(
    raw: &dto::ValueType,
    path: FieldPath,
) -> Result<ConnectorValueType, crate::ProtocolError> {
    let kind = dto::ValueTypeKind::try_from(raw.kind)
        .map_err(|_| invalid_enum(path.clone().field("kind"), "unknown value type kind"))?;

    let decimal_present = raw.decimal_precision.is_some() || raw.decimal_scale.is_some();
    let fixed_present = raw.fixed_length.is_some();

    let value_type = match kind {
        dto::ValueTypeKind::Unspecified => {
            return Err(invalid_enum(
                path.field("kind"),
                "value type kind must be specified",
            ));
        }
        dto::ValueTypeKind::Decimal => {
            let precision = raw.decimal_precision.ok_or_else(|| {
                missing(
                    path.clone().field("decimal_precision"),
                    "decimal value type requires a precision",
                )
            })?;
            let scale = raw.decimal_scale.ok_or_else(|| {
                missing(
                    path.clone().field("decimal_scale"),
                    "decimal value type requires a scale",
                )
            })?;
            if precision == 0 || precision > MAX_DECIMAL_PRECISION {
                return Err(out_of_range(
                    path.clone().field("decimal_precision"),
                    "decimal precision must be within 1..=38",
                ));
            }
            if scale < 0 || scale > precision as i32 {
                return Err(out_of_range(
                    path.clone().field("decimal_scale"),
                    "decimal scale must be within 0..=precision",
                ));
            }
            if fixed_present {
                return Err(inconsistent(
                    path.field("fixed_length"),
                    "decimal value type must not carry a fixed length",
                ));
            }
            ConnectorValueType::Decimal {
                precision: precision as u8,
                scale: scale as i8,
            }
        }
        dto::ValueTypeKind::Fixed => {
            let length = raw.fixed_length.ok_or_else(|| {
                missing(
                    path.clone().field("fixed_length"),
                    "fixed value type requires a length",
                )
            })?;
            if length == 0 || length as usize > MAX_SCALAR_BYTES {
                return Err(out_of_range(
                    path.clone().field("fixed_length"),
                    "fixed length must be positive and within the scalar limit",
                ));
            }
            if decimal_present {
                return Err(inconsistent(
                    path.field("decimal_precision"),
                    "fixed value type must not carry decimal parameters",
                ));
            }
            ConnectorValueType::Fixed { length }
        }
        simple => {
            if decimal_present {
                return Err(inconsistent(
                    path.clone().field("decimal_precision"),
                    "value type must not carry decimal parameters",
                ));
            }
            if fixed_present {
                return Err(inconsistent(
                    path.field("fixed_length"),
                    "value type must not carry a fixed length",
                ));
            }
            match simple {
                dto::ValueTypeKind::Boolean => ConnectorValueType::Boolean,
                dto::ValueTypeKind::TinyInt => ConnectorValueType::TinyInt,
                dto::ValueTypeKind::SmallInt => ConnectorValueType::SmallInt,
                dto::ValueTypeKind::Integer => ConnectorValueType::Integer,
                dto::ValueTypeKind::BigInt => ConnectorValueType::BigInt,
                dto::ValueTypeKind::Real => ConnectorValueType::Real,
                dto::ValueTypeKind::Double => ConnectorValueType::Double,
                dto::ValueTypeKind::Date => ConnectorValueType::Date,
                dto::ValueTypeKind::TimeMicros => ConnectorValueType::TimeMicros,
                dto::ValueTypeKind::TimestampMicros => ConnectorValueType::TimestampMicros,
                dto::ValueTypeKind::TimestampMillis => ConnectorValueType::TimestampMillis,
                dto::ValueTypeKind::TimestampTzMicros => ConnectorValueType::TimestampTzMicros,
                dto::ValueTypeKind::TimestampNanos => ConnectorValueType::TimestampNanos,
                dto::ValueTypeKind::TimestampTzNanos => ConnectorValueType::TimestampTzNanos,
                dto::ValueTypeKind::Varchar => ConnectorValueType::Varchar,
                dto::ValueTypeKind::Varbinary => ConnectorValueType::Varbinary,
                dto::ValueTypeKind::Uuid => ConnectorValueType::Uuid,
                dto::ValueTypeKind::NonComparable => ConnectorValueType::NonComparable,
                dto::ValueTypeKind::Unspecified
                | dto::ValueTypeKind::Decimal
                | dto::ValueTypeKind::Fixed => unreachable!("handled above"),
            }
        }
    };
    Ok(value_type)
}

/// Encode an SPI value type as its unique generated representation.
pub fn encode_value_type(value_type: ConnectorValueType) -> dto::ValueType {
    let mut encoded = dto::ValueType {
        kind: dto::ValueTypeKind::Unspecified as i32,
        decimal_precision: None,
        decimal_scale: None,
        fixed_length: None,
    };
    encoded.kind = match value_type {
        ConnectorValueType::NonComparable => dto::ValueTypeKind::NonComparable,
        ConnectorValueType::Boolean => dto::ValueTypeKind::Boolean,
        ConnectorValueType::TinyInt => dto::ValueTypeKind::TinyInt,
        ConnectorValueType::SmallInt => dto::ValueTypeKind::SmallInt,
        ConnectorValueType::Integer => dto::ValueTypeKind::Integer,
        ConnectorValueType::BigInt => dto::ValueTypeKind::BigInt,
        ConnectorValueType::Real => dto::ValueTypeKind::Real,
        ConnectorValueType::Double => dto::ValueTypeKind::Double,
        ConnectorValueType::Decimal { precision, scale } => {
            encoded.decimal_precision = Some(u32::from(precision));
            encoded.decimal_scale = Some(i32::from(scale));
            dto::ValueTypeKind::Decimal
        }
        ConnectorValueType::Date => dto::ValueTypeKind::Date,
        ConnectorValueType::TimeMicros => dto::ValueTypeKind::TimeMicros,
        ConnectorValueType::TimestampMicros => dto::ValueTypeKind::TimestampMicros,
        ConnectorValueType::TimestampMillis => dto::ValueTypeKind::TimestampMillis,
        ConnectorValueType::TimestampTzMicros => dto::ValueTypeKind::TimestampTzMicros,
        ConnectorValueType::TimestampNanos => dto::ValueTypeKind::TimestampNanos,
        ConnectorValueType::TimestampTzNanos => dto::ValueTypeKind::TimestampTzNanos,
        ConnectorValueType::Varchar => dto::ValueTypeKind::Varchar,
        ConnectorValueType::Varbinary => dto::ValueTypeKind::Varbinary,
        ConnectorValueType::Uuid => dto::ValueTypeKind::Uuid,
        ConnectorValueType::Fixed { length } => {
            encoded.fixed_length = Some(length);
            dto::ValueTypeKind::Fixed
        }
    } as i32;
    encoded
}

/// Decode a value and check it against the exact type it must inhabit.
pub fn decode_value(
    raw: &dto::Value,
    expected: ConnectorValueType,
    path: FieldPath,
) -> Result<ConnectorValue, crate::ProtocolError> {
    let value = raw
        .value
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "value must be present"))?;
    let decoded = match value {
        dto::value::Value::Boolean(value) => ConnectorValue::Boolean(*value),
        // The wire carries it as a 32-bit varint because proto has no narrower
        // signed integer; a value outside the eight-bit range is a producer
        // bug, not a value to truncate.
        dto::value::Value::TinyInt(value) => {
            ConnectorValue::TinyInt(i8::try_from(*value).map_err(|_| {
                out_of_range(
                    path.clone().field("tiny_int"),
                    "tiny int value must be within -128..=127",
                )
            })?)
        }
        dto::value::Value::SmallInt(value) => {
            ConnectorValue::SmallInt(i16::try_from(*value).map_err(|_| {
                out_of_range(
                    path.clone().field("small_int"),
                    "small int value must be within -32768..=32767",
                )
            })?)
        }
        dto::value::Value::Integer(value) => ConnectorValue::Integer(*value),
        dto::value::Value::BigInt(value) => ConnectorValue::BigInt(*value),
        dto::value::Value::Real(value) => ConnectorValue::Real(*value),
        dto::value::Value::DoubleValue(value) => ConnectorValue::Double(*value),
        dto::value::Value::Decimal(value) => {
            exact_bytes(&value.unscaled, 16, path.clone().field("decimal"))?;
            if value.precision == 0 || value.precision > MAX_DECIMAL_PRECISION {
                return Err(out_of_range(
                    path.clone().field("decimal"),
                    "decimal precision must be within 1..=38",
                ));
            }
            if value.scale < 0 || value.scale > value.precision as i32 {
                return Err(out_of_range(
                    path.clone().field("decimal"),
                    "decimal scale must be within 0..=precision",
                ));
            }
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&value.unscaled);
            ConnectorValue::Decimal {
                unscaled: i128::from_be_bytes(bytes),
                precision: value.precision as u8,
                scale: value.scale as i8,
            }
        }
        dto::value::Value::Date(value) => ConnectorValue::Date(*value),
        dto::value::Value::TimeMicros(value) => ConnectorValue::TimeMicros(*value),
        dto::value::Value::TimestampMicros(value) => ConnectorValue::TimestampMicros(*value),
        dto::value::Value::TimestampMillis(value) => ConnectorValue::TimestampMillis(*value),
        dto::value::Value::TimestampTzMicros(value) => ConnectorValue::TimestampTzMicros(*value),
        dto::value::Value::TimestampNanos(value) => ConnectorValue::TimestampNanos(*value),
        dto::value::Value::TimestampTzNanos(value) => ConnectorValue::TimestampTzNanos(*value),
        dto::value::Value::Varchar(value) => {
            bounded_text(value, MAX_SCALAR_BYTES, path.clone().field("varchar"), true)?;
            ConnectorValue::Varchar(Arc::from(value.as_str()))
        }
        dto::value::Value::Varbinary(value) => {
            bounded_bytes(value, MAX_SCALAR_BYTES, path.clone().field("varbinary"))?;
            ConnectorValue::Varbinary(Arc::from(value.as_slice()))
        }
        dto::value::Value::Uuid(value) => {
            exact_bytes(value, 16, path.clone().field("uuid"))?;
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(value);
            ConnectorValue::Uuid(bytes)
        }
        dto::value::Value::Fixed(value) => {
            bounded_bytes(value, MAX_SCALAR_BYTES, path.clone().field("fixed"))?;
            ConnectorValue::Fixed(Arc::from(value.as_slice()))
        }
    };
    if decoded.value_type() != expected {
        return Err(invalid(
            path,
            "value type differs from the type it is declared under",
        ));
    }
    Ok(decoded)
}

/// Encode an SPI value as its unique generated representation.
pub fn encode_value(value: &ConnectorValue) -> dto::Value {
    let encoded = match value {
        ConnectorValue::Boolean(value) => dto::value::Value::Boolean(*value),
        ConnectorValue::TinyInt(value) => dto::value::Value::TinyInt(i32::from(*value)),
        ConnectorValue::SmallInt(value) => dto::value::Value::SmallInt(i32::from(*value)),
        ConnectorValue::Integer(value) => dto::value::Value::Integer(*value),
        ConnectorValue::BigInt(value) => dto::value::Value::BigInt(*value),
        ConnectorValue::Real(value) => dto::value::Value::Real(*value),
        ConnectorValue::Double(value) => dto::value::Value::DoubleValue(*value),
        ConnectorValue::Decimal {
            unscaled,
            precision,
            scale,
        } => dto::value::Value::Decimal(dto::DecimalValue {
            unscaled: unscaled.to_be_bytes().to_vec(),
            precision: u32::from(*precision),
            scale: i32::from(*scale),
        }),
        ConnectorValue::Date(value) => dto::value::Value::Date(*value),
        ConnectorValue::TimeMicros(value) => dto::value::Value::TimeMicros(*value),
        ConnectorValue::TimestampMicros(value) => dto::value::Value::TimestampMicros(*value),
        ConnectorValue::TimestampMillis(value) => dto::value::Value::TimestampMillis(*value),
        ConnectorValue::TimestampTzMicros(value) => dto::value::Value::TimestampTzMicros(*value),
        ConnectorValue::TimestampNanos(value) => dto::value::Value::TimestampNanos(*value),
        ConnectorValue::TimestampTzNanos(value) => dto::value::Value::TimestampTzNanos(*value),
        ConnectorValue::Varchar(value) => dto::value::Value::Varchar(value.to_string()),
        ConnectorValue::Varbinary(value) => dto::value::Value::Varbinary(value.to_vec()),
        ConnectorValue::Uuid(value) => dto::value::Value::Uuid(value.to_vec()),
        ConnectorValue::Fixed(value) => dto::value::Value::Fixed(value.to_vec()),
    };
    dto::Value {
        value: Some(encoded),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> FieldPath {
        FieldPath::root("value")
    }

    #[test]
    fn every_value_type_round_trips() {
        for value_type in [
            ConnectorValueType::Boolean,
            ConnectorValueType::TinyInt,
            ConnectorValueType::SmallInt,
            ConnectorValueType::Integer,
            ConnectorValueType::BigInt,
            ConnectorValueType::Real,
            ConnectorValueType::Double,
            ConnectorValueType::Decimal {
                precision: 18,
                scale: 4,
            },
            ConnectorValueType::Date,
            ConnectorValueType::TimeMicros,
            ConnectorValueType::TimestampMicros,
            ConnectorValueType::TimestampMillis,
            ConnectorValueType::TimestampTzMicros,
            ConnectorValueType::TimestampNanos,
            ConnectorValueType::TimestampTzNanos,
            ConnectorValueType::Varchar,
            ConnectorValueType::Varbinary,
            ConnectorValueType::Uuid,
            ConnectorValueType::Fixed { length: 12 },
        ] {
            let encoded = encode_value_type(value_type);
            let decoded = decode_value_type(&encoded, root()).expect("valid type");
            assert_eq!(decoded, value_type);
        }
    }

    #[test]
    fn unspecified_and_unknown_kinds_are_rejected() {
        let unspecified = dto::ValueType {
            kind: dto::ValueTypeKind::Unspecified as i32,
            decimal_precision: None,
            decimal_scale: None,
            fixed_length: None,
        };
        let error = decode_value_type(&unspecified, root()).expect_err("unspecified");
        assert_eq!(error.kind(), crate::ProtocolErrorKind::InvalidEnum);

        let unknown = dto::ValueType {
            kind: 9999,
            ..unspecified
        };
        assert_eq!(
            decode_value_type(&unknown, root())
                .expect_err("unknown")
                .kind(),
            crate::ProtocolErrorKind::InvalidEnum
        );
    }

    #[test]
    fn decimal_parameters_must_match_the_kind() {
        let missing_scale = dto::ValueType {
            kind: dto::ValueTypeKind::Decimal as i32,
            decimal_precision: Some(10),
            decimal_scale: None,
            fixed_length: None,
        };
        assert_eq!(
            decode_value_type(&missing_scale, root())
                .expect_err("missing scale")
                .kind(),
            crate::ProtocolErrorKind::MissingField
        );

        let stray = dto::ValueType {
            kind: dto::ValueTypeKind::BigInt as i32,
            decimal_precision: Some(10),
            decimal_scale: Some(2),
            fixed_length: None,
        };
        assert_eq!(
            decode_value_type(&stray, root())
                .expect_err("stray parameters")
                .kind(),
            crate::ProtocolErrorKind::InconsistentFields
        );

        let out_of_range_scale = dto::ValueType {
            kind: dto::ValueTypeKind::Decimal as i32,
            decimal_precision: Some(10),
            decimal_scale: Some(11),
            fixed_length: None,
        };
        assert_eq!(
            decode_value_type(&out_of_range_scale, root())
                .expect_err("scale")
                .kind(),
            crate::ProtocolErrorKind::OutOfRange
        );
    }

    #[test]
    fn values_round_trip_and_reject_a_type_mismatch() {
        for value in [
            ConnectorValue::SmallInt(i16::MIN),
            ConnectorValue::SmallInt(i16::MAX),
            ConnectorValue::TimestampMillis(-1_704_067_200_123),
            ConnectorValue::TimestampMillis(1_704_067_200_123),
        ] {
            let encoded = encode_value(&value);
            assert_eq!(
                decode_value(&encoded, value.value_type(), root()).expect("valid"),
                value
            );
        }

        let value = ConnectorValue::BigInt(42);
        let encoded = encode_value(&value);
        assert_eq!(
            decode_value(&encoded, ConnectorValueType::Integer, root())
                .expect_err("mismatch")
                .kind(),
            crate::ProtocolErrorKind::InvalidValue
        );

        let out_of_range = dto::Value {
            value: Some(dto::value::Value::SmallInt(i32::from(i16::MAX) + 1)),
        };
        assert_eq!(
            decode_value(&out_of_range, ConnectorValueType::SmallInt, root())
                .expect_err("out of range")
                .kind(),
            crate::ProtocolErrorKind::OutOfRange
        );
    }

    #[test]
    fn fixed_width_values_carry_their_width_in_the_type() {
        let value = ConnectorValue::Fixed(Arc::from(&[1_u8, 2, 3][..]));
        let encoded = encode_value(&value);
        assert!(decode_value(&encoded, ConnectorValueType::Fixed { length: 3 }, root()).is_ok());
        assert!(decode_value(&encoded, ConnectorValueType::Fixed { length: 4 }, root()).is_err());
    }

    #[test]
    fn oversized_scalars_are_rejected_before_use() {
        let oversized = dto::Value {
            value: Some(dto::value::Value::Varbinary(vec![
                0_u8;
                MAX_SCALAR_BYTES + 1
            ])),
        };
        assert_eq!(
            decode_value(&oversized, ConnectorValueType::Varbinary, root())
                .expect_err("oversized")
                .kind(),
            crate::ProtocolErrorKind::OutOfRange
        );
    }

    #[test]
    fn a_uuid_must_be_exactly_sixteen_bytes() {
        let short = dto::Value {
            value: Some(dto::value::Value::Uuid(vec![0_u8; 15])),
        };
        assert_eq!(
            decode_value(&short, ConnectorValueType::Uuid, root())
                .expect_err("short uuid")
                .kind(),
            crate::ProtocolErrorKind::InvalidValue
        );
    }

    #[test]
    fn an_absent_value_is_a_missing_field() {
        let empty = dto::Value { value: None };
        assert_eq!(
            decode_value(&empty, ConnectorValueType::BigInt, root())
                .expect_err("absent")
                .kind(),
            crate::ProtocolErrorKind::MissingField
        );
    }
}
