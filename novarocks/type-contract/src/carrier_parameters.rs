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

use arrow_schema::{
    ArrowError, DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE, DECIMAL64_MAX_PRECISION,
    DECIMAL64_MAX_SCALE, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
    DECIMAL256_MAX_SCALE, DataType, TimeUnit,
};
use std::fmt;

#[derive(Debug)]
pub enum CarrierParameterError {
    Invalid(&'static str),
    Decimal(ArrowError),
}
impl fmt::Display for CarrierParameterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::Decimal(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for CarrierParameterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Invalid(_) => None,
            Self::Decimal(error) => Some(error),
        }
    }
}

/// Validate one Arrow type node's carrier parameters, observing the node and
/// each Union type id. The caller owns complete tree traversal, metadata and
/// depth limits; this function does not recurse or authorize allocation.
pub fn validate_arrow_carrier_parameters_observed<E: From<CarrierParameterError>>(
    ty: &DataType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    observe()?;
    let decimal = match ty {
        DataType::Decimal32(p, s) => Some((*p, *s, DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE)),
        DataType::Decimal64(p, s) => Some((*p, *s, DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE)),
        DataType::Decimal128(p, s) => {
            Some((*p, *s, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE))
        }
        DataType::Decimal256(p, s) => {
            Some((*p, *s, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE))
        }
        _ => None,
    };
    if let Some((precision, scale, max_precision, max_scale)) = decimal {
        validate_decimal_parameters(precision, scale, max_precision, max_scale)
            .map_err(CarrierParameterError::Decimal)?;
    }
    let invalid = match ty {
        DataType::Time32(unit) => !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond),
        DataType::Time64(unit) => !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond),
        DataType::FixedSizeBinary(width) | DataType::FixedSizeList(_, width) => *width < 0,
        DataType::Dictionary(key, _) => !matches!(
            key.as_ref(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ),
        DataType::RunEndEncoded(ends, _) => {
            ends.is_nullable()
                || !matches!(
                    ends.data_type(),
                    DataType::Int16 | DataType::Int32 | DataType::Int64
                )
        }
        DataType::Map(entries, _) => {
            entries.is_nullable()
                || !matches!(entries.data_type(), DataType::Struct(fields) if fields.len() == 2)
        }
        DataType::Union(fields, _) => {
            if fields.len() > 128 {
                return Err(CarrierParameterError::Invalid("too many Union type ids").into());
            }
            let mut ids = [false; 128];
            for (id, _) in fields.iter() {
                observe()?;
                if id < 0 || ids[id as usize] {
                    return Err(CarrierParameterError::Invalid(
                        "invalid or duplicate Union type id",
                    )
                    .into());
                }
                ids[id as usize] = true;
            }
            false
        }
        _ => false,
    };
    if invalid {
        Err(CarrierParameterError::Invalid("invalid Arrow constant carrier parameters").into())
    } else {
        Ok(())
    }
}

// Preserve the exact Arrow 58.2 decimal parameter grammar and error order
// without pulling Arrow arrays into this schema-only contract crate.
fn validate_decimal_parameters(
    precision: u8,
    scale: i8,
    max_precision: u8,
    max_scale: i8,
) -> Result<(), ArrowError> {
    if precision == 0 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "precision cannot be 0, has to be between [1, {max_precision}]"
        )));
    }
    if precision > max_precision {
        return Err(ArrowError::InvalidArgumentError(format!(
            "precision {precision} is greater than max {max_precision}"
        )));
    }
    if scale > max_scale {
        return Err(ArrowError::InvalidArgumentError(format!(
            "scale {scale} is greater than max {max_scale}"
        )));
    }
    if scale > 0 && scale as u8 > precision {
        return Err(ArrowError::InvalidArgumentError(format!(
            "scale {scale} is greater than precision {precision}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, UnionFields, UnionMode};
    use std::sync::Arc;

    fn validate(ty: &DataType) -> Result<(), CarrierParameterError> {
        validate_arrow_carrier_parameters_observed(ty, || Ok(()))
    }
    type DecimalConstructor = fn(u8, i8) -> DataType;
    const DECIMALS: [(DecimalConstructor, u8, i8); 4] = [
        (DataType::Decimal32, 9, 9),
        (DataType::Decimal64, 18, 18),
        (DataType::Decimal128, 38, 38),
        (DataType::Decimal256, 76, 76),
    ];
    fn field(ty: DataType, nullable: bool) -> Arc<Field> {
        Arc::new(Field::new("v", ty, nullable))
    }
    fn union(ids: impl IntoIterator<Item = i8>) -> DataType {
        let fields: UnionFields = ids
            .into_iter()
            .map(|id| (id, field(DataType::Int32, true)))
            .collect();
        DataType::Union(fields, UnionMode::Dense)
    }
    fn map(entries_nullable: bool, fields: Vec<Arc<Field>>) -> DataType {
        DataType::Map(
            field(DataType::Struct(fields.into()), entries_nullable),
            false,
        )
    }

    #[test]
    fn decimal_boundaries_include_all_negative_scales_without_absolute_limit() {
        for (constructor, max_precision, max_scale) in DECIMALS {
            for precision in [1, max_precision] {
                for scale in i8::MIN..=0 {
                    validate(&constructor(precision, scale)).unwrap();
                }
            }
            validate(&constructor(1, 1)).unwrap();
            validate(&constructor(max_precision, max_scale)).unwrap();
        }
    }

    #[test]
    fn decimal_invalid_boundaries_preserve_arrow_variant_text_and_precedence() {
        for (constructor, max_precision, max_scale) in DECIMALS {
            let cases = [
                (
                    0,
                    i8::MAX,
                    format!("precision cannot be 0, has to be between [1, {max_precision}]"),
                ),
                (
                    max_precision + 1,
                    i8::MAX,
                    format!(
                        "precision {} is greater than max {max_precision}",
                        max_precision + 1
                    ),
                ),
                (
                    u8::MAX,
                    i8::MIN,
                    format!("precision 255 is greater than max {max_precision}"),
                ),
                (
                    1,
                    max_scale + 1,
                    format!("scale {} is greater than max {max_scale}", max_scale + 1),
                ),
                (
                    max_precision,
                    i8::MAX,
                    format!("scale 127 is greater than max {max_scale}"),
                ),
                (1, 2, "scale 2 is greater than precision 1".to_string()),
            ];
            for (precision, scale, expected) in cases {
                match validate(&constructor(precision, scale)).unwrap_err() {
                    CarrierParameterError::Decimal(ArrowError::InvalidArgumentError(message)) => {
                        assert_eq!(message, expected)
                    }
                    error => panic!("wrong decimal error: {error:?}"),
                }
            }
        }
    }

    #[test]
    fn carrier_parameter_grammar_accepts_all_supported_node_boundaries() {
        for ty in [
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeList(field(DataType::Int32, true), 0),
            map(
                false,
                vec![field(DataType::Utf8, false), field(DataType::Int32, true)],
            ),
            map(
                false,
                vec![field(DataType::Utf8, true), field(DataType::Int32, true)],
            ),
            union([0, 127]),
            union([]),
        ] {
            validate(&ty).unwrap();
        }
        for key in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            validate(&DataType::Dictionary(
                Box::new(key),
                Box::new(DataType::Utf8),
            ))
            .unwrap();
        }
        for ends in [DataType::Int16, DataType::Int32, DataType::Int64] {
            validate(&DataType::RunEndEncoded(
                field(ends, false),
                field(DataType::Utf8, true),
            ))
            .unwrap();
        }
    }

    #[test]
    fn carrier_parameter_grammar_rejects_invalid_time_width_dictionary_run_end_and_map() {
        let mut invalid = vec![
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::FixedSizeBinary(-1),
            DataType::FixedSizeList(field(DataType::Int32, true), -1),
            DataType::RunEndEncoded(field(DataType::Int16, true), field(DataType::Utf8, true)),
            DataType::RunEndEncoded(field(DataType::Int8, false), field(DataType::Utf8, true)),
            DataType::RunEndEncoded(field(DataType::UInt32, false), field(DataType::Utf8, true)),
            DataType::Map(field(DataType::Int32, false), false),
            map(
                true,
                vec![field(DataType::Utf8, false), field(DataType::Int32, true)],
            ),
            map(false, vec![]),
            map(false, vec![field(DataType::Utf8, false)]),
        ];
        for key in [
            DataType::Boolean,
            DataType::Float64,
            DataType::Utf8,
            DataType::Date32,
        ] {
            invalid.push(DataType::Dictionary(
                Box::new(key),
                Box::new(DataType::Int32),
            ));
        }
        for ty in invalid {
            assert!(matches!(
                validate(&ty),
                Err(CarrierParameterError::Invalid(
                    "invalid Arrow constant carrier parameters"
                ))
            ));
        }
    }

    #[test]
    fn public_union_grammar_and_valid_identity_walk_keep_their_own_phases() {
        // Invalid IDs cannot inhabit a safely constructed current UnionFields.
        for (ids, expected) in [
            (vec![-1], "type ids must be non-negative: -1"),
            (vec![0, 0], "duplicate type id: 0"),
        ] {
            let children = ids.iter().map(|_| field(DataType::Int32, true));
            let error = UnionFields::try_new(ids.clone(), children).unwrap_err();
            assert!(
                matches!(error, arrow_schema::ArrowError::InvalidArgumentError(actual) if actual == expected)
            );
        }
        let error = UnionFields::try_from_fields((0..129).map(|_| field(DataType::Int32, true)))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument error: UnionFields cannot contain more than 128 fields"
        );
        let mut calls = 0;
        validate_arrow_carrier_parameters_observed(&union(0..=127), || {
            calls += 1;
            Ok::<_, CarrierParameterError>(())
        })
        .unwrap();
        assert_eq!(calls, 129);
    }

    #[derive(Debug)]
    enum ObservedError {
        Carrier(CarrierParameterError),
        Control(crate::CompileControlError),
    }
    impl From<CarrierParameterError> for ObservedError {
        fn from(error: CarrierParameterError) -> Self {
            Self::Carrier(error)
        }
    }
    #[test]
    fn observer_control_errors_preserve_node_entry_and_each_union_id() {
        for error in [
            crate::CompileControlError::Cancelled,
            crate::CompileControlError::DeadlineExceeded,
            crate::CompileControlError::ResourceExhausted,
        ] {
            let entry =
                validate_arrow_carrier_parameters_observed(&DataType::Decimal32(0, 127), || {
                    Err::<(), _>(ObservedError::Control(error.clone()))
                });
            assert!(matches!(entry, Err(ObservedError::Control(actual)) if actual == error));
            let mut calls = 0;
            let result = validate_arrow_carrier_parameters_observed(&union([0, 1, 2]), || {
                calls += 1;
                if calls == 3 {
                    Err(ObservedError::Control(error.clone()))
                } else {
                    Ok(())
                }
            });
            assert_eq!(calls, 3);
            assert!(matches!(result, Err(ObservedError::Control(actual)) if actual == error));
        }
        let result = validate_arrow_carrier_parameters_observed(
            &DataType::Time32(TimeUnit::Nanosecond),
            || Ok::<_, ObservedError>(()),
        );
        assert!(matches!(
            result,
            Err(ObservedError::Carrier(CarrierParameterError::Invalid(_)))
        ));
    }

    #[test]
    fn node_validation_does_not_recurse_or_replace_the_callers_tree_checks() {
        let invalid_child = DataType::Decimal128(0, 0);
        validate(&DataType::List(field(invalid_child.clone(), true))).unwrap();
        assert!(matches!(
            validate(&invalid_child),
            Err(CarrierParameterError::Decimal(_))
        ));
    }
}
