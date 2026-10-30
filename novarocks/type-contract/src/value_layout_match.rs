// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed comparison of an immutable plain layout and its actual encoding.

use crate::{CarrierParameterError, ValueLayout, ValueTypeError};
use arrow_schema::{DataType, Field};

/// Compare actual value shape to a checked, normalized immutable layout.
///
/// Dictionary and run-end wrappers are validated, then traversed as runtime
/// encoding. This port grants neither a kernel capability nor memory capacity.
/// Work reports each visited node/field/parameter and the actual compared UTF8
/// byte count, in chunks of at most 1024 bytes. No normalized tree is allocated.
pub fn value_layout_matches_carrier_observed<E: From<ValueTypeError>>(
    expected: &ValueLayout,
    actual: &DataType,
    observe: impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    data_type_layout_matches_carrier_observed(expected.data_type(), actual, observe)
}

/// Compare to an already borrowed normalized plain value layout.
///
/// The caller supplies the same canonical shape as `ValueLayout::data_type()`;
/// expected runtime encoding wrappers are rejected. Metadata, dictionary IPC
/// descriptors and anonymous role labels do not participate. Scalar parameters,
/// offset widths, named fields, child nullability, Map order and Union IDs/mode
/// remain exact. Caller refusal returns unchanged before any subsequent work.
pub fn data_type_layout_matches_carrier_observed<E: From<ValueTypeError>>(
    expected: &DataType,
    actual: &DataType,
    mut observe: impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    Walk {
        observe: &mut observe,
        nodes: 0,
    }
    .ty(expected, actual, 1)
}

// Design: ADR-0195 (docs/adr/ADR-0195-public-value-access-and-construction.md)
/// Frozen string layouts have one canonical dictionary-value storage layout.
/// This is a storage fact, not a local consumer capability or wire identity.
pub fn dictionary_value_layout(layout: &DataType) -> Option<DataType> {
    match layout {
        DataType::Utf8 | DataType::Utf8View => Some(DataType::Utf8),
        DataType::LargeUtf8 => Some(DataType::LargeUtf8),
        _ => None,
    }
}

enum ParameterFailure<E> {
    Caller(E),
    Carrier(CarrierParameterError),
}
impl<E> From<CarrierParameterError> for ParameterFailure<E> {
    fn from(error: CarrierParameterError) -> Self {
        Self::Carrier(error)
    }
}

struct Walk<'a, F> {
    observe: &'a mut F,
    nodes: usize,
}
impl<E: From<ValueTypeError>, F: FnMut(usize) -> Result<(), E>> Walk<'_, F> {
    fn node(&mut self, actual: &DataType, depth: usize) -> Result<(), E> {
        (self.observe)(1)?;
        if depth > crate::MAX_VALUE_TYPE_DEPTH {
            return Err(ValueTypeError::TooDeep.into());
        }
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| E::from(ValueTypeError::TooManyNodes))?;
        if self.nodes > crate::MAX_VALUE_TYPE_NODES {
            return Err(ValueTypeError::TooManyNodes.into());
        }
        crate::validate_arrow_carrier_parameters_observed::<ParameterFailure<E>>(actual, || {
            (self.observe)(1).map_err(ParameterFailure::Caller)
        })
        .map_err(|error| match error {
            ParameterFailure::Caller(error) => error,
            ParameterFailure::Carrier(CarrierParameterError::Invalid(message)) => {
                ValueTypeError::InvalidType(message).into()
            }
            ParameterFailure::Carrier(CarrierParameterError::Decimal(_)) => {
                ValueTypeError::InvalidType("invalid decimal carrier parameters").into()
            }
        })
    }

    fn bytes(&mut self, expected: &[u8], actual: &[u8]) -> Result<bool, E> {
        (self.observe)(1)?;
        if expected.len() != actual.len() {
            return Ok(false);
        }
        for (expected, actual) in expected.chunks(1024).zip(actual.chunks(1024)) {
            (self.observe)(actual.len())?;
            if expected != actual {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn field(
        &mut self,
        expected: &Field,
        actual: &Field,
        named: bool,
        depth: usize,
    ) -> Result<bool, E> {
        (self.observe)(1)?;
        if expected.is_nullable() != actual.is_nullable() {
            return Ok(false);
        }
        if named && !self.bytes(expected.name().as_bytes(), actual.name().as_bytes())? {
            return Ok(false);
        }
        self.ty(expected.data_type(), actual.data_type(), depth)
    }

    fn ty(&mut self, expected: &DataType, actual: &DataType, depth: usize) -> Result<bool, E> {
        self.node(actual, depth)?;
        match actual {
            DataType::Dictionary(keys, values) => {
                self.node(keys, depth + 1)?;
                let dictionary_layout = dictionary_value_layout(expected);
                return self.ty(
                    dictionary_layout.as_ref().unwrap_or(expected),
                    values,
                    depth + 1,
                );
            }
            DataType::RunEndEncoded(ends, values) => {
                (self.observe)(1)?;
                self.node(ends.data_type(), depth + 1)?;
                (self.observe)(1)?;
                return self.ty(expected, values.data_type(), depth + 1);
            }
            _ => {}
        }
        Ok(match (expected, actual) {
            (DataType::List(expected), DataType::List(actual))
            | (DataType::LargeList(expected), DataType::LargeList(actual))
            | (DataType::ListView(expected), DataType::ListView(actual))
            | (DataType::LargeListView(expected), DataType::LargeListView(actual)) => {
                self.field(expected, actual, false, depth + 1)?
            }
            (DataType::FixedSizeList(expected, el), DataType::FixedSizeList(actual, al)) => {
                (self.observe)(1)?;
                el == al && self.field(expected, actual, false, depth + 1)?
            }
            (DataType::Struct(expected), DataType::Struct(actual)) => {
                (self.observe)(1)?;
                if expected.len() != actual.len() {
                    return Ok(false);
                }
                for (expected, actual) in expected.iter().zip(actual) {
                    if !self.field(expected, actual, true, depth + 1)? {
                        return Ok(false);
                    }
                }
                true
            }
            (DataType::Map(expected, eo), DataType::Map(actual, ao)) => {
                (self.observe)(1)?;
                if eo != ao || expected.is_nullable() != actual.is_nullable() {
                    return Ok(false);
                }
                self.node(actual.data_type(), depth + 1)?;
                let (DataType::Struct(expected), DataType::Struct(actual)) =
                    (expected.data_type(), actual.data_type())
                else {
                    return Ok(false);
                };
                // The entries Struct is a physical container with anonymous
                // key/value role fields, not an independent named value.
                (self.observe)(1)?;
                if expected.len() != 2 || actual.len() != 2 {
                    return Ok(false);
                }
                self.field(&expected[0], &actual[0], false, depth + 2)?
                    && self.field(&expected[1], &actual[1], false, depth + 2)?
            }
            (DataType::Union(expected, em), DataType::Union(actual, am)) => {
                (self.observe)(1)?;
                if em != am || expected.len() != actual.len() {
                    return Ok(false);
                }
                for ((ei, expected), (ai, actual)) in expected.iter().zip(actual.iter()) {
                    (self.observe)(1)?;
                    if ei != ai || !self.field(expected, actual, true, depth + 1)? {
                        return Ok(false);
                    }
                }
                true
            }
            (DataType::Timestamp(eu, et), DataType::Timestamp(au, at)) => {
                (self.observe)(1)?;
                if eu != au {
                    return Ok(false);
                }
                match (et, at) {
                    (None, None) => true,
                    (Some(expected), Some(actual)) => {
                        self.bytes(expected.as_bytes(), actual.as_bytes())?
                    }
                    _ => false,
                }
            }
            (DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _), _) => false,
            _ => {
                // Every remaining supported pair is a fixed scalar parameter
                // comparison. No metadata maps or variable field names occur.
                (self.observe)(1)?;
                expected == actual
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValueTypeStorageLifetime;
    use arrow_schema::{Field, TimeUnit, UnionFields, UnionMode};
    use std::sync::Arc;
    fn matches(expected: &DataType, actual: &DataType) -> Result<bool, ValueTypeError> {
        let layout = ValueLayout::try_new(expected, ValueTypeStorageLifetime::pure()).unwrap();
        value_layout_matches_carrier_observed(&layout, actual, |_| Ok(()))
    }
    fn dictionary(values: DataType) -> DataType {
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(values))
    }
    #[test]
    fn encoding_and_annotations_are_orthogonal_to_precise_nested_layout() {
        let expected = DataType::Struct(
            vec![Field::new(
                "named",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            )]
            .into(),
        );
        let actual = DataType::Struct(
            vec![Field::new(
                "named",
                DataType::List(Arc::new(
                    Field::new("provider_role", dictionary(DataType::Utf8), true).with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            String::from("format"),
                            String::from("fact"),
                        )]),
                    ),
                )),
                false,
            )]
            .into(),
        );
        assert_eq!(matches(&expected, &actual), Ok(true));
        let actual = DataType::Struct(
            vec![Field::new(
                "renamed",
                DataType::List(Arc::new(Field::new(
                    "role",
                    dictionary(DataType::Utf8),
                    true,
                ))),
                false,
            )]
            .into(),
        );
        assert_eq!(matches(&expected, &actual), Ok(false));
        let actual = DataType::Struct(
            vec![Field::new(
                "named",
                DataType::LargeList(Arc::new(Field::new(
                    "role",
                    dictionary(DataType::Utf8),
                    true,
                ))),
                false,
            )]
            .into(),
        );
        assert_eq!(matches(&expected, &actual), Ok(false));
    }
    #[test]
    fn null_scalar_parameters_and_map_order_are_precise() {
        assert_eq!(
            matches(
                &DataType::Decimal128(12, 2),
                &dictionary(DataType::Decimal128(12, 3))
            ),
            Ok(false)
        );
        assert_eq!(
            matches(
                &DataType::FixedSizeBinary(16),
                &DataType::FixedSizeBinary(15)
            ),
            Ok(false)
        );
        assert_eq!(
            matches(
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("utc".into()))
            ),
            Ok(false)
        );
        let map = |label: &str, nullable, ordered| {
            DataType::Map(
                Arc::new(Field::new(
                    label,
                    DataType::Struct(
                        vec![
                            Field::new("external_key", DataType::Utf8, false),
                            Field::new("external_value", dictionary(DataType::Utf8), nullable),
                        ]
                        .into(),
                    ),
                    false,
                )),
                ordered,
            )
        };
        let plain = ValueLayout::try_new(
            &map("foreign", true, false),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        assert_eq!(
            value_layout_matches_carrier_observed::<ValueTypeError>(
                &plain,
                &map("another", true, false),
                |_| Ok(())
            ),
            Ok(true)
        );
        assert_eq!(
            value_layout_matches_carrier_observed::<ValueTypeError>(
                &plain,
                &map("another", false, false),
                |_| Ok(())
            ),
            Ok(false)
        );
        assert_eq!(
            value_layout_matches_carrier_observed::<ValueTypeError>(
                &plain,
                &map("another", true, true),
                |_| Ok(())
            ),
            Ok(false)
        );
    }
    #[test]
    fn valid_run_end_encoding_is_borrowed_and_invalid_wrappers_are_rejected() {
        let run = |ends: DataType, nullable| {
            DataType::RunEndEncoded(
                Arc::new(Field::new("provider_ends", ends, nullable)),
                Arc::new(Field::new(
                    "provider_values",
                    dictionary(DataType::Utf8),
                    true,
                )),
            )
        };
        assert_eq!(
            matches(&DataType::Utf8, &run(DataType::Int16, false)),
            Ok(true)
        );
        assert!(matches(&DataType::Utf8, &run(DataType::Int16, true)).is_err());
        assert!(matches(&DataType::Utf8, &run(DataType::UInt16, false)).is_err());
        assert!(
            matches(
                &DataType::Utf8,
                &DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Utf8))
            )
            .is_err()
        );
    }
    #[test]
    fn union_ids_modes_names_and_child_null_are_precise() {
        let union = |id, name: &str, nullable, mode| {
            DataType::Union(
                UnionFields::try_new(
                    [id],
                    [Field::new(name, dictionary(DataType::Utf8), nullable)],
                )
                .unwrap(),
                mode,
            )
        };
        let layout = ValueLayout::try_new(
            &union(3, "named", true, UnionMode::Dense),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        for actual in [
            union(4, "named", true, UnionMode::Dense),
            union(3, "other", true, UnionMode::Dense),
            union(3, "named", false, UnionMode::Dense),
            union(3, "named", true, UnionMode::Sparse),
        ] {
            assert!(
                !value_layout_matches_carrier_observed::<ValueTypeError>(&layout, &actual, |_| Ok(
                    ()
                ))
                .unwrap()
            );
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    enum Failure {
        Refused,
        Type(ValueTypeError),
    }
    impl From<ValueTypeError> for Failure {
        fn from(error: ValueTypeError) -> Self {
            Self::Type(error)
        }
    }
    #[test]
    fn actual_utf8_byte_work_is_observed_and_refusal_has_no_later_work() {
        let name = "界".repeat(300);
        let expected = DataType::Struct(vec![Field::new(&name, DataType::Int32, false)].into());
        let layout = ValueLayout::try_new(&expected, ValueTypeStorageLifetime::pure()).unwrap();
        let mut work = Vec::new();
        assert!(
            value_layout_matches_carrier_observed::<ValueTypeError>(&layout, &expected, |units| {
                work.push(units);
                Ok(())
            })
            .unwrap()
        );
        assert_eq!(
            work.iter().filter(|units| **units > 1).sum::<usize>(),
            name.len()
        );
        let mut calls = 0;
        let error = value_layout_matches_carrier_observed::<Failure>(&layout, &expected, |_| {
            calls += 1;
            Err(Failure::Refused)
        });
        assert_eq!(error, Err(Failure::Refused));
        assert_eq!(calls, 1);
        for stop in 1..=work.len() {
            let mut calls = 0;
            let error =
                value_layout_matches_carrier_observed::<Failure>(&layout, &expected, |_| {
                    calls += 1;
                    if calls == stop {
                        Err(Failure::Refused)
                    } else {
                        Ok(())
                    }
                });
            assert_eq!(error, Err(Failure::Refused));
            assert_eq!(calls, stop);
        }
    }
}

#[cfg(test)]
mod dictionary_layout_tests {
    use super::*;
    #[test]
    fn frozen_view_dictionary_matches_canonical_offsets_only() {
        for (layout, value) in [
            (DataType::Utf8, DataType::Utf8),
            (DataType::LargeUtf8, DataType::LargeUtf8),
            (DataType::Utf8View, DataType::Utf8),
        ] {
            assert_eq!(dictionary_value_layout(&layout), Some(value.clone()));
            let carrier = DataType::Dictionary(Box::new(DataType::Int32), Box::new(value));
            assert!(
                data_type_layout_matches_carrier_observed::<ValueTypeError>(
                    &layout,
                    &carrier,
                    |_| Ok(())
                )
                .unwrap()
            );
        }
        let unsupported =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View));
        assert!(
            !data_type_layout_matches_carrier_observed::<ValueTypeError>(
                &DataType::Utf8View,
                &unsupported,
                |_| Ok(())
            )
            .unwrap()
        );
        assert!(
            !data_type_layout_matches_carrier_observed::<ValueTypeError>(
                &DataType::Utf8View,
                &DataType::Utf8,
                |_| Ok(())
            )
            .unwrap()
        );
        assert_eq!(dictionary_value_layout(&DataType::BinaryView), None);
    }
}
