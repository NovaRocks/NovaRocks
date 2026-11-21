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
//! Original v1 literal value and builder, and the sole native NEGATE zero author.
use arrow_buffer::i256;
#[derive(Clone, Debug)]
pub enum LegacyLiteralValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Utf8(String),
    Binary(Vec<u8>),
    Date32(i32),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        value: i256,
        precision: u8,
        scale: i8,
    },
}

use crate::largeint;
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, NullArray,
    StringArray,
};
use std::sync::Arc;

pub fn eval(value: &LegacyLiteralValue, len: usize) -> Result<ArrayRef, crate::KernelFailure> {
    preflight_literal(value, len)?;
    match value {
        LegacyLiteralValue::Null => Ok(Arc::new(NullArray::new(len))),
        LegacyLiteralValue::Int8(v) => {
            let arr = Int8Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Int16(v) => {
            let arr = Int16Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Int32(v) => {
            let arr = Int32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Bool(v) => {
            let arr = BooleanArray::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Int64(v) => {
            let arr = Int64Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::LargeInt(v) => {
            let values = vec![Some(*v); len];
            match largeint::array_from_i128_observed(&values, &mut |_| {
                Ok::<(), std::convert::Infallible>(())
            }) {
                Ok(array) => Ok(array),
                Err(never) => match never {},
            }
        }
        LegacyLiteralValue::Float32(v) => {
            let arr = Float32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Float64(v) => {
            let arr = Float64Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Utf8(v) => {
            let arr = StringArray::from(vec![v.as_str(); len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Binary(v) => {
            let values = std::iter::repeat_n(v.as_slice(), len).collect::<Vec<&[u8]>>();
            let arr = BinaryArray::from_vec(values);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Date32(v) => {
            let arr = Date32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let arr = Decimal128Array::from(vec![*value; len])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|cause| {
                    crate::KernelFailure::from_domain(LiteralFailure::Decimal(cause))
                })?;
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Decimal256 {
            value,
            precision,
            scale,
        } => {
            let arr = Decimal256Array::from(vec![*value; len])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|cause| {
                    crate::KernelFailure::from_domain(LiteralFailure::Decimal(cause))
                })?;
            Ok(Arc::new(arr))
        }
    }
}

#[derive(Debug)]
enum LiteralFailure {
    Decimal(arrow_schema::ArrowError),
    Bytes { required: u128 },
    Offsets { unit: usize, total: u128 },
}
impl std::fmt::Display for LiteralFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decimal(cause) => write!(f, "literal decimal carrier: {cause}"),
            Self::Bytes { required } => write!(
                f,
                "literal backing requires {required} bytes, maximum {}",
                isize::MAX
            ),
            Self::Offsets { unit, total } => write!(
                f,
                "literal offset extent exceeded: one={unit} output={total}"
            ),
        }
    }
}
impl std::error::Error for LiteralFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decimal(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for LiteralFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, code) = match self {
            Self::Decimal(_) => (FailureClass::Invariant, 198),
            Self::Bytes { .. } => (FailureClass::Representation, 199),
            Self::Offsets { .. } => (FailureClass::Representation, 200),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        match self {
            Self::Decimal(_) => fact,
            Self::Bytes { required } => crate::allocation_failure::rust_array_backing_failure_fact(
                fact.reason(),
                *required,
                RepresentationBoundary::SingleOutput,
            ),
            Self::Offsets { unit, total } => {
                let single = *unit as u128 > i32::MAX as u128;
                fact.with_representation(RepresentationFacts {
                    boundary: if single {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: std::num::NonZeroU16::new(32).unwrap(),
                    },
                    required: u64::try_from(if single { *unit as u128 } else { *total }).ok(),
                    extent_limit: Some(i32::MAX as u64),
                    representable_prefix: None,
                })
            }
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for LiteralFailure {}
fn literal_bytes(required: u128) -> Result<(), crate::KernelFailure> {
    if required > isize::MAX as u128 {
        Err(crate::KernelFailure::from_domain(LiteralFailure::Bytes {
            required,
        }))
    } else {
        Ok(())
    }
}
fn literal_offsets(unit: usize, rows: usize) -> Result<(), crate::KernelFailure> {
    if rows == 0 {
        return Ok(());
    }
    let total = unit as u128 * rows as u128;
    if total > i32::MAX as u128 {
        return Err(crate::KernelFailure::from_domain(LiteralFailure::Offsets {
            unit,
            total,
        }));
    }
    literal_bytes((rows as u128 + 1) * std::mem::size_of::<i32>() as u128)
}
fn preflight_literal(value: &LegacyLiteralValue, rows: usize) -> Result<(), crate::KernelFailure> {
    use std::mem::size_of;
    let element = match value {
        LegacyLiteralValue::Null => return Ok(()),
        LegacyLiteralValue::Int8(_) | LegacyLiteralValue::Bool(_) => 1,
        LegacyLiteralValue::Int16(_) => 2,
        LegacyLiteralValue::Int32(_)
        | LegacyLiteralValue::Float32(_)
        | LegacyLiteralValue::Date32(_) => 4,
        LegacyLiteralValue::Int64(_) | LegacyLiteralValue::Float64(_) => 8,
        LegacyLiteralValue::Decimal128 { .. } => size_of::<i128>(),
        LegacyLiteralValue::Decimal256 { .. } => size_of::<i256>(),
        LegacyLiteralValue::LargeInt(_) => size_of::<Option<i128>>(),
        LegacyLiteralValue::Utf8(value) => {
            literal_offsets(value.len(), rows)?;
            size_of::<&str>()
        }
        LegacyLiteralValue::Binary(value) => {
            literal_offsets(value.len(), rows)?;
            size_of::<&[u8]>()
        }
    };
    literal_bytes(rows as u128 * element as u128)
}

#[cfg(test)]
mod domain_tests {
    use super::*;
    use arrow_array::Array;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn literal_extent_refuses_before_large_materialization() {
        let error = eval(&LegacyLiteralValue::Utf8("x".into()), i32::MAX as usize + 1).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        let representation = fact.representation().unwrap();
        assert_eq!(
            representation.boundary,
            RepresentationBoundary::SingleOutput
        );
        assert_eq!(representation.required, Some(i32::MAX as u64 + 1));
        let unit = literal_offsets(i32::MAX as usize + 1, 1).unwrap_err();
        assert_eq!(
            unit.failure_fact().representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
        assert!(literal_offsets(usize::MAX, 0).is_ok());
        let capacity = eval(&LegacyLiteralValue::Int64(7), usize::MAX).unwrap_err();
        assert_eq!(
            capacity.failure_fact().class(),
            FailureClass::Representation
        );
        assert!(capacity.failure_fact().resource().is_none());
        let extent = capacity.failure_fact().representation().unwrap();
        assert_eq!(extent.required, None);
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
    }
    #[test]
    fn literal_decimal_retains_actual_arrow_error_and_binary_bytes() {
        use std::error::Error;
        let arrow = Decimal128Array::from(vec![1])
            .with_precision_and_scale(0, 0)
            .unwrap_err();
        let wrapped = LiteralFailure::Decimal(arrow);
        assert!(
            wrapped
                .source()
                .unwrap()
                .downcast_ref::<arrow_schema::ArrowError>()
                .is_some()
        );
        let error = eval(
            &LegacyLiteralValue::Decimal128 {
                value: 1,
                precision: 0,
                scale: 0,
            },
            1,
        )
        .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        let value = eval(&LegacyLiteralValue::Binary(vec![0, 255, 3]), 3).unwrap();
        let value = value.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(value.len(), 3);
        for row in 0..3 {
            assert_eq!(value.value(row), &[0, 255, 3]);
        }
    }
}
