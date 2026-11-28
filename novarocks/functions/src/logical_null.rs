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

//! Shared borrowed logical NULL access; no computed bitmap or hidden allocation.

use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, DictionaryArray, RunArray, UnionArray};
use arrow_schema::DataType;

/// A malformed frozen carrier/address; contains no borrowed input graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NullAddressViolation {
    NestedAddress,
    DictionaryCarrier,
    DictionaryKey,
    RunCarrier,
    RunIndex,
    UnionCarrier,
    UnionTypeId,
}
impl std::fmt::Display for NullAddressViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NestedAddress => "kernel value has invalid nested row addressing",
            Self::DictionaryCarrier => "kernel dictionary carrier differs from its type",
            Self::DictionaryKey => "kernel dictionary key type is invalid",
            Self::RunCarrier => "kernel run-end carrier differs from its type",
            Self::RunIndex => "kernel run-end index type is invalid",
            Self::UnionCarrier => "kernel union carrier differs from its type",
            Self::UnionTypeId => "kernel union type id is invalid",
        })
    }
}
impl std::error::Error for NullAddressViolation {}
impl novarocks_failure_contract::FailureProjection for NullAddressViolation {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self {
            Self::NestedAddress => 160,
            Self::DictionaryCarrier => 161,
            Self::DictionaryKey => 162,
            Self::RunCarrier => 163,
            Self::RunIndex => 164,
            Self::UnionCarrier => 165,
            Self::UnionTypeId => 166,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for NullAddressViolation {}

/// Borrowed logical-NULL visitor failure. Observation refusal stays primary.
#[derive(Debug, PartialEq, Eq)]
pub enum NullVisitError<E> {
    Control(E),
    Invalid(NullAddressViolation),
}
impl<E> From<E> for NullVisitError<E> {
    fn from(error: E) -> Self {
        Self::Control(error)
    }
}

pub(crate) fn logical_is_null_observed<E>(
    array: &dyn Array,
    row: usize,
    depth: usize,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, NullVisitError<E>> {
    logical_is_null_at(array, row, depth.max(1), false, observe)
}

fn logical_is_null_at<E>(
    array: &dyn Array,
    row: usize,
    depth: usize,
    parent_encoding: bool,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, NullVisitError<E>> {
    observe().map_err(NullVisitError::Control)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH || row >= array.len() {
        return Err(NullVisitError::Invalid(NullAddressViolation::NestedAddress));
    }
    if parent_encoding
        && matches!(
            array.data_type(),
            DataType::Dictionary(..) | DataType::RunEndEncoded(..)
        )
    {
        return Err(NullVisitError::Invalid(NullAddressViolation::NestedAddress));
    }
    if array.is_null(row) {
        return Ok(true);
    }
    match array.data_type() {
        DataType::Null => Ok(true),
        DataType::Dictionary(key, _) => {
            macro_rules! dictionary {
                ($key:ty) => {{
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<$key>>()
                        .ok_or_else(|| {
                            NullVisitError::Invalid(NullAddressViolation::DictionaryCarrier)
                        })?;
                    match array.key(row) {
                        Some(key) => {
                            logical_is_null_at(array.values().as_ref(), key, depth, true, observe)
                        }
                        None => Ok(true),
                    }
                }};
            }
            match key.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                _ => Err(NullVisitError::Invalid(NullAddressViolation::DictionaryKey)),
            }
        }
        DataType::RunEndEncoded(run_ends, _) => {
            macro_rules! run {
                ($key:ty) => {{
                    let array = array
                        .as_any()
                        .downcast_ref::<RunArray<$key>>()
                        .ok_or_else(|| NullVisitError::Invalid(NullAddressViolation::RunCarrier))?;
                    logical_is_null_at(
                        array.values().as_ref(),
                        array.get_physical_index(row),
                        depth,
                        true,
                        observe,
                    )
                }};
            }
            match run_ends.data_type() {
                DataType::Int16 => run!(Int16Type),
                DataType::Int32 => run!(Int32Type),
                DataType::Int64 => run!(Int64Type),
                _ => Err(NullVisitError::Invalid(NullAddressViolation::RunIndex)),
            }
        }
        DataType::Union(fields, _) => {
            let array = array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(|| NullVisitError::Invalid(NullAddressViolation::UnionCarrier))?;
            let type_id = array.type_id(row);
            let mut known = false;
            for (id, _) in fields.iter() {
                observe().map_err(NullVisitError::Control)?;
                if id == type_id {
                    known = true;
                    break;
                }
            }
            if !known {
                return Err(NullVisitError::Invalid(NullAddressViolation::UnionTypeId));
            }
            logical_is_null_at(
                array.child(type_id).as_ref(),
                array.value_offset(row),
                depth + 1,
                false,
                observe,
            )
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests;
