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

use arrow_schema::DataType;

use crate::{
    CharacterKind, CharacterType, TemporalType, ValueKind, ValueLogicalType, ValueTypeError,
    ValueTypeRef,
};

/// Relations used for complete, typed values. SQL equality can be unknown.
/// Identity is a total equivalence, including for NULL and NaN.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyRelation {
    Equal,
    Identical,
}

/// A nested MAP lookup can be indeterminate even when its result is consumed
/// by a larger comparison that can still prove inequality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyComparison {
    True,
    False,
    Unknown,
    Indeterminate,
}

impl From<bool> for KeyComparison {
    fn from(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedKeyType {
    Json,
    Variant,
    Nominal(ValueLogicalType),
    Scalar,
    Union,
}

/// Leaf interpretation comes from the complete value declaration. A physical
/// byte width or an Arrow field annotation never assigns nominal semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyLeaf {
    Null,
    Boolean,
    Signed,
    Float32,
    Float64,
    Decimal128,
    Decimal256,
    Bytes,
    Character(CharacterType),
    FixedBytes,
    Temporal(TemporalType),
}

pub fn key_leaf(ty: &ValueTypeRef) -> Result<Option<KeyLeaf>, UnsupportedKeyType> {
    let leaf = match ty.kind() {
        ValueKind::List(_)
        | ValueKind::FixedSizeList { .. }
        | ValueKind::Struct(_)
        | ValueKind::Map { .. } => return Ok(None),
        ValueKind::Union(_) => return Err(UnsupportedKeyType::Union),
        ValueKind::String | ValueKind::Binary => KeyLeaf::Bytes,
        ValueKind::BoundedCharacter(character) => KeyLeaf::Character(*character),
        ValueKind::Temporal(temporal) => KeyLeaf::Temporal(*temporal),
        ValueKind::Nominal(ValueLogicalType::LargeInt | ValueLogicalType::Uuid) => {
            KeyLeaf::FixedBytes
        }
        ValueKind::Nominal(ValueLogicalType::Json) => return Err(UnsupportedKeyType::Json),
        ValueKind::Nominal(ValueLogicalType::Variant) => return Err(UnsupportedKeyType::Variant),
        ValueKind::Nominal(nominal) => return Err(UnsupportedKeyType::Nominal(*nominal)),
        ValueKind::Scalar(carrier) => match carrier {
            DataType::Null => KeyLeaf::Null,
            DataType::Boolean => KeyLeaf::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Timestamp(_, _) => KeyLeaf::Signed,
            DataType::Float32 => KeyLeaf::Float32,
            DataType::Float64 => KeyLeaf::Float64,
            DataType::Decimal128(_, _) => KeyLeaf::Decimal128,
            DataType::Decimal256(_, _) => KeyLeaf::Decimal256,
            DataType::FixedSizeBinary(_) => KeyLeaf::FixedBytes,
            _ => return Err(UnsupportedKeyType::Scalar),
        },
    };
    Ok(Some(leaf))
}

impl KeyRelation {
    /// Validate the entire typed domain independently of any sampled values.
    /// The nested result keeps capability refusal separate from caller control.
    pub fn validate_type_observed<E>(
        self,
        ty: &ValueTypeRef,
        mut observe: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Result<(), UnsupportedKeyType>, E> {
        fn visit<E>(
            ty: &ValueTypeRef,
            observe: &mut impl FnMut(usize) -> Result<(), E>,
        ) -> Result<Result<(), UnsupportedKeyType>, E> {
            observe(1)?;
            match key_leaf(ty) {
                Ok(Some(_)) => return Ok(Ok(())),
                Err(error) => return Ok(Err(error)),
                Ok(None) => {}
            }
            for index in 0..ty.child_count() {
                let result = visit(ty.child_type_at(index).expect("declared child"), observe)?;
                if result.is_err() {
                    return Ok(result);
                }
            }
            Ok(Ok(()))
        }

        visit(ty, &mut observe)
    }
}

/// Compare the complete value domains consumed by an already selected
/// relation. The values retain independent layout and NULL declarations.
/// ROW comparisons address fields by their position.
pub fn key_domains_match_observed<E: From<ValueTypeError>>(
    left: &ValueTypeRef,
    right: &ValueTypeRef,
    mut observe: impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    fn matches<E: From<ValueTypeError>>(
        left: &ValueTypeRef,
        right: &ValueTypeRef,
        observe: &mut impl FnMut(usize) -> Result<(), E>,
    ) -> Result<bool, E> {
        observe(1)?;
        let local = match (left.kind(), right.kind()) {
            (ValueKind::List(_), ValueKind::List(_))
            | (ValueKind::Map { .. }, ValueKind::Map { .. }) => true,
            (
                ValueKind::FixedSizeList { length: a, .. },
                ValueKind::FixedSizeList { length: b, .. },
            ) => a == b,
            (ValueKind::Struct(a), ValueKind::Struct(b)) => a.len() == b.len(),
            (ValueKind::Scalar(a), ValueKind::Scalar(b)) => {
                crate::arrow_data_types_equal_borrowed_observed(a, b, || observe(1))?
            }
            (ValueKind::Temporal(a), ValueKind::Temporal(b)) => a == b,
            (ValueKind::BoundedCharacter(a), ValueKind::BoundedCharacter(b)) => a == b,
            (ValueKind::Nominal(a), ValueKind::Nominal(b)) => a == b,
            (ValueKind::String, ValueKind::String) | (ValueKind::Binary, ValueKind::Binary) => true,
            _ => false,
        };
        if !local {
            return Ok(false);
        }
        for index in 0..left.child_count() {
            if !matches(
                left.child_type_at(index).expect("checked shape"),
                right.child_type_at(index).expect("checked shape"),
                observe,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    matches(left, right, &mut observe)
}

/// The key bytes for an already validated character value. CHAR removes
/// trailing U+0020 padding. VARCHAR retains all source bytes.
pub fn key_character_bytes_observed<E>(
    character: CharacterType,
    mut bytes: &[u8],
    mut observe: impl FnMut(usize) -> Result<(), E>,
) -> Result<&[u8], E> {
    if character.kind() == CharacterKind::Varchar {
        return Ok(bytes);
    }
    while !bytes.is_empty() {
        let block = bytes.len().min(64 * 1024);
        observe(block)?;
        if let Some(last) = bytes[bytes.len() - block..]
            .iter()
            .rposition(|&byte| byte != b' ')
        {
            return Ok(&bytes[..bytes.len() - block + last + 1]);
        }
        bytes = &bytes[..bytes.len() - block];
    }
    Ok(bytes)
}

/// The common `IDENTICAL` float leaf of the existing grouping and map domains.
pub fn canonical_f64_bits(value: f64) -> u64 {
    if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// See [`canonical_f64_bits`].
pub fn canonical_f32_bits(value: f32) -> u32 {
    if value.is_nan() {
        0x7fc0_0000
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// Frozen ordinary scalar operation; never interpreted from a runtime name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
impl ComparisonOperator {
    pub fn apply(self, ordering: std::cmp::Ordering) -> bool {
        match self {
            Self::Eq => ordering == std::cmp::Ordering::Equal,
            Self::Ne => ordering != std::cmp::Ordering::Equal,
            Self::Lt => ordering == std::cmp::Ordering::Less,
            Self::Le => ordering != std::cmp::Ordering::Greater,
            Self::Gt => ordering == std::cmp::Ordering::Greater,
            Self::Ge => ordering != std::cmp::Ordering::Less,
        }
    }
    pub fn is_ordering(self) -> bool {
        matches!(self, Self::Lt | Self::Le | Self::Gt | Self::Ge)
    }
}

/// The equality relation proved by the author of an extracted join key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum JoinKeyMatch {
    Grouping,
    IeeeFloat,
}

impl JoinKeyMatch {
    pub fn supports(
        self,
        carrier: &DataType,
        domain: crate::ValueLogicalType,
        null_safe: bool,
    ) -> bool {
        match self {
            Self::Grouping => true,
            Self::IeeeFloat => {
                !null_safe
                    && domain == crate::ValueLogicalType::Physical
                    && matches!(carrier, DataType::Float32 | DataType::Float64)
            }
        }
    }
}

/// Complete identity of an ordered value comparison algorithm.
///
/// A variant freezes scalar interpretation and equality consistency. Direction
/// and NULL placement remain properties of the ordered key. Any semantic
/// change requires a new variant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OrderedComparisonAlgorithm {
    /// NovaRocks scalar comparison. LARGEINT and Decimal128 compare as signed
    /// integers, physical Date32 and Timestamp use their Arrow epoch values,
    /// and Utf8 compares by unsigned UTF-8 bytes. Ordered runtime filters
    /// consume nominal temporal relation keys. The key supplies NULL placement.
    NativeScalarOrderV1,
}

impl OrderedComparisonAlgorithm {
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::NativeScalarOrderV1 => "novarocks.native-scalar-order.v1",
        }
    }

    /// Capability for a complete logical value domain. A signed LARGEINT
    /// comparison cannot be borrowed by an unrelated binary carrier.
    pub fn supports_value_type(self, ty: &crate::FunctionValueType) -> bool {
        let identity = if crate::is_largeint_data_type(ty.data_type()) {
            ty.logical_type() == crate::ValueLogicalType::LargeInt
        } else {
            ty.logical_type() == crate::ValueLogicalType::Physical
        };
        identity && self.supports_order_key(ty.data_type())
    }

    /// Ordered runtime filters interpret a frozen temporal declaration through
    /// its relation key. The declaration distinguishes same-width carriers.
    pub fn supports_runtime_filter_key(self, ty: &crate::FunctionValueType) -> bool {
        match (self, ty.value_type().temporal()) {
            (Self::NativeScalarOrderV1, Some(temporal)) => temporal.carrier() == *ty.data_type(),
            (_, None) => self.supports_value_type(ty),
        }
    }

    /// Reports the exact Arrow scalar domain frozen by this revision.
    ///
    /// Floating-point types are excluded until NaN and signed-zero behavior is
    /// frozen across planning, reduction and execution. Unsigned, nested,
    /// dictionary, general binary and alternate string or temporal carriers
    /// likewise require a future explicit algorithm revision.
    pub fn supports_order_key(self, data_type: &DataType) -> bool {
        match self {
            Self::NativeScalarOrderV1 => {
                matches!(
                    data_type,
                    DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Utf8
                        | DataType::Date32
                        | DataType::Timestamp(_, _)
                        | DataType::Decimal128(_, _)
                ) || crate::is_largeint_data_type(data_type)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::OrderedComparisonAlgorithm;

    #[test]
    fn complete_key_capability_keeps_known_unsupported_nominal_domains() {
        use super::*;
        use crate::{FunctionValueType, ValueTypeStorageLifetime};

        let json = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Json,
        )
        .unwrap();
        let inner = ValueTypeRef::try_new(
            ValueKind::List(json.value_type().clone()),
            true,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let mut visited = 0;
        let mut observe = |n| {
            visited += n;
            Ok::<_, &str>(())
        };
        assert_eq!(
            KeyRelation::Equal.validate_type_observed(&inner, &mut observe),
            Ok(Err(UnsupportedKeyType::Json))
        );
        assert_eq!(visited, 2);
        assert_eq!(
            KeyRelation::Identical.validate_type_observed(&inner, |_| Err("original work")),
            Err("original work")
        );
        let string = FunctionValueType::new(DataType::Utf8, true);
        assert_eq!(key_leaf(string.value_type()), Ok(Some(KeyLeaf::Bytes)));
    }

    #[test]
    fn shared_float_leaves_preserve_the_existing_grouping_bits() {
        use super::*;
        assert_eq!(canonical_f32_bits(-0.0), 0);
        assert_eq!(canonical_f32_bits(f32::from_bits(0xffc0_005a)), 0x7fc0_0000);
        assert_eq!(canonical_f32_bits(-5.0), (-5.0f32).to_bits());
        assert_eq!(canonical_f64_bits(-0.0), 0);
        assert_eq!(
            canonical_f64_bits(f64::from_bits(0xfff8_0000_0000_005a)),
            0x7ff8_0000_0000_0000
        );
        assert_eq!(canonical_f64_bits(-5.0), (-5.0f64).to_bits());
    }

    #[test]
    fn native_scalar_comparator_has_stable_identity_and_closed_domain() {
        let algorithm = OrderedComparisonAlgorithm::NativeScalarOrderV1;
        assert_eq!(algorithm.stable_name(), "novarocks.native-scalar-order.v1");
        assert!(algorithm.supports_order_key(&DataType::Int64));
        assert!(algorithm.supports_order_key(&DataType::Boolean));
        assert!(algorithm.supports_order_key(&DataType::Utf8));
        assert!(algorithm.supports_order_key(&DataType::Decimal128(18, 4)));
        assert!(
            algorithm.supports_order_key(&DataType::FixedSizeBinary(crate::LARGEINT_BYTE_WIDTH))
        );

        for unsupported in [
            DataType::Float64,
            DataType::UInt64,
            DataType::Binary,
            DataType::Date64,
            DataType::List(arrow_schema::Field::new_list_field(DataType::Int64, false).into()),
        ] {
            assert!(!algorithm.supports_order_key(&unsupported));
        }
    }

    #[test]
    fn ordered_runtime_filters_admit_the_typed_temporal_key() {
        use crate::{
            FunctionValueType, TemporalPrecision, TemporalType, ValueKind, ValueLayout,
            ValueTypeRef, ValueTypeStorageLifetime,
        };

        let algorithm = OrderedComparisonAlgorithm::NativeScalarOrderV1;
        let storage = ValueTypeStorageLifetime::pure();
        for declaration in [
            TemporalType::Timestamp(TemporalPrecision::MICROSECONDS),
            TemporalType::Timestamp(TemporalPrecision::PICOSECONDS),
            TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS),
        ] {
            let carrier = declaration.carrier();
            let typed = FunctionValueType::try_from_value(
                ValueTypeRef::try_new(ValueKind::Temporal(declaration), false, storage.clone())
                    .unwrap(),
                ValueLayout::try_new(&carrier, storage.clone()).unwrap(),
            )
            .unwrap();
            assert!(algorithm.supports_runtime_filter_key(&typed));
            if matches!(carrier, DataType::FixedSizeBinary(_)) {
                assert!(!algorithm.supports_value_type(&typed));
                let untyped = FunctionValueType::new(carrier, false);
                assert!(!algorithm.supports_runtime_filter_key(&untyped));
            }
        }
    }
}
