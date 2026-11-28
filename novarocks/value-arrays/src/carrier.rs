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

//! The engine byte-carrier table. General Arrow operations do not use this gate.
use crate::{ValueError, ValueFailure, ValueViolation};
use arrow_schema::DataType;
use novarocks_failure_contract::{
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use novarocks_type_contract::{
    ArrowTypeProfile, ValueTypeError, visit_arrow_layout_nodes_observed,
};
use std::num::NonZeroU16;

/// Stable identifiers; extents use payload bytes (offsets) or one value's bytes
/// (views). Descriptor counts and address regions are checked independently.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(u16)]
pub enum ByteLayout {
    Utf8 = 1,
    LargeUtf8 = 2,
    Utf8View = 3,
    Binary = 4,
    LargeBinary = 5,
    BinaryView = 6,
}
const LAYOUTS: [(ByteLayout, DataType); 6] = [
    (ByteLayout::Utf8, DataType::Utf8),
    (ByteLayout::LargeUtf8, DataType::LargeUtf8),
    (ByteLayout::Utf8View, DataType::Utf8View),
    (ByteLayout::Binary, DataType::Binary),
    (ByteLayout::LargeBinary, DataType::LargeBinary),
    (ByteLayout::BinaryView, DataType::BinaryView),
];
impl ByteLayout {
    pub fn from_data_type(ty: &DataType) -> Option<Self> {
        LAYOUTS.iter().find(|(_, t)| t == ty).map(|(l, _)| *l)
    }
    pub fn data_type(self) -> DataType {
        LAYOUTS.iter().find(|(l, _)| *l == self).unwrap().1.clone()
    }
    pub const fn is_utf8(self) -> bool {
        matches!(self, Self::Utf8 | Self::LargeUtf8 | Self::Utf8View)
    }
    pub const fn is_view(self) -> bool {
        matches!(self, Self::Utf8View | Self::BinaryView)
    }
    pub const fn extent_limit(self) -> u64 {
        match self {
            Self::LargeUtf8 | Self::LargeBinary => i64::MAX as u64,
            _ => i32::MAX as u64,
        }
    }
    pub fn representation(
        self,
        boundary: RepresentationBoundary,
        required: Option<u64>,
        extent_limit: Option<u64>,
        representable_prefix: Option<u64>,
    ) -> RepresentationFacts {
        RepresentationFacts {
            boundary,
            layout: RepresentationLayout {
                domain: domain::VALUE_ARRAYS,
                code: NonZeroU16::new(self as u16).unwrap(),
            },
            required,
            extent_limit,
            representable_prefix,
        }
    }
    pub fn dictionary_value_layout(self) -> Option<DataType> {
        novarocks_type_contract::dictionary_value_layout(&self.data_type())
    }
}

/// Validate only the engine's active set, preserving control callback identity.
/// Scalar parameters and semantic bindings are validated by their type owner.
pub fn validate_engine_carrier_observed<E>(
    ty: &DataType,
    observe: &mut impl FnMut(&DataType) -> Result<(), E>,
) -> Result<(), ValueFailure<E>> {
    visit_carrier_shape(ty, |ty| {
        observe(ty).map_err(ValueFailure::Caller)?;
        let violation = match ty {
            DataType::Dictionary(keys, values) => {
                let supported = [
                    ByteLayout::Utf8,
                    ByteLayout::LargeUtf8,
                    ByteLayout::Utf8View,
                ]
                .iter()
                .filter_map(|layout| layout.dictionary_value_layout())
                .any(|value| &value == values.as_ref());
                (keys.as_ref() != &DataType::Int32 || !supported)
                    .then_some(ValueViolation::Dictionary)
            }
            DataType::RunEndEncoded(..) | DataType::ListView(_) | DataType::LargeListView(_) => {
                Some(ValueViolation::Carrier)
            }
            _ => None,
        };
        match violation {
            Some(violation) => Err(ValueError::Invariant(violation).into()),
            None => Ok(()),
        }
    })
}

pub(crate) fn visit_carrier_shape<E>(
    ty: &DataType,
    mut visit: impl FnMut(&DataType) -> Result<(), ValueFailure<E>>,
) -> Result<(), ValueFailure<E>> {
    enum VisitError<E> {
        Type(ValueTypeError),
        Carrier(ValueFailure<E>),
    }
    impl<E> From<ValueTypeError> for VisitError<E> {
        fn from(error: ValueTypeError) -> Self {
            Self::Type(error)
        }
    }

    visit_arrow_layout_nodes_observed(ty, ArrowTypeProfile::Value, |ty, _| {
        visit(ty).map_err(VisitError::Carrier)
    })
    .map_err(|error| match error {
        VisitError::Type(ValueTypeError::TooDeep) => {
            ValueError::Invariant(ValueViolation::Depth).into()
        }
        VisitError::Type(_) => ValueError::Invariant(ValueViolation::Carrier).into(),
        VisitError::Carrier(error) => error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_carriers_and_canonical_dictionary_table() {
        for (_, ty) in LAYOUTS {
            assert!(
                validate_engine_carrier_observed::<std::convert::Infallible>(&ty, &mut |_| Ok(()))
                    .is_ok()
            );
        }
        for value in [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Binary,
        ] {
            let valid = matches!(value, DataType::Utf8 | DataType::LargeUtf8);
            let carrier = DataType::Dictionary(Box::new(DataType::Int32), Box::new(value));
            assert_eq!(
                validate_engine_carrier_observed::<std::convert::Infallible>(
                    &carrier,
                    &mut |_| Ok(())
                )
                .is_ok(),
                valid
            );
        }
    }
    #[test]
    fn caller_refusal_remains_the_original_value() {
        assert!(matches!(
            validate_engine_carrier_observed(&DataType::Utf8, &mut |_| Err(41)),
            Err(ValueFailure::Caller(41))
        ));
    }
}
