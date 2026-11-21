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
    /// integers, Date32 and Timestamp compare by their Arrow physical epoch
    /// representation, and Utf8 compares by unsigned UTF-8 bytes. NULL
    /// placement is supplied by the key.
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
}
