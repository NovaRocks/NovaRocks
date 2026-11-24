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

use crate::KernelFailure;
use crate::aggregate_scalar::{ScalarStateAllocator, ScalarWork, TrackedAggScalarValue};
use novarocks_type_contract::{
    FunctionValueType, KeyComparison, KeyLeaf, KeyRelation, UnsupportedKeyType,
};
use novarocks_value_arrays::{
    ValueError, ValueFailure, ValueViolation,
    hash_eq::{
        KeyPreparationFailure, PreparedKeyOperations, RetainedKeyChild, RetainedKeyContents,
        RetainedKeyValue,
    },
};

/// Complete original K. An unsupported reader is reported at the first
/// admitted key, so root-NULL/empty groups keep the ordinary aggregate policy.
#[derive(Clone, Debug)]
pub(in crate::builtin) struct KeyIdentity(Result<PreparedKeyOperations, UnsupportedKeyType>);

impl KeyIdentity {
    pub(in crate::builtin) fn prepare<E>(
        declaration: &FunctionValueType,
        work: &mut novarocks_value_arrays::control::Work<'_, E>,
    ) -> Result<Self, E> {
        Ok(Self(
            match PreparedKeyOperations::prepare_observed(declaration, KeyRelation::Identical, work)
            {
                Ok(key) => Ok(key),
                Err(KeyPreparationFailure::Unsupported(cause)) => Err(cause),
                Err(KeyPreparationFailure::Caller(cause)) => return Err(cause),
            },
        ))
    }

    pub(super) fn hash<A: ScalarStateAllocator>(
        &self,
        key: &TrackedAggScalarValue<A>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<u64, KernelFailure> {
        let operation = self
            .0
            .as_ref()
            .map_err(|cause| super::failure::key_type(*cause))?;
        operation
            .hash_retained_observed(key, &mut |count| observe(work, count))
            .map_err(|cause| match cause {
                ValueFailure::Caller(cause) => cause,
                ValueFailure::Value(cause) => KernelFailure::from_domain(cause),
            })
    }

    pub(super) fn matches<A: ScalarStateAllocator>(
        &self,
        left: &TrackedAggScalarValue<A>,
        right: &TrackedAggScalarValue<A>,
        allocator: &A,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<bool, KernelFailure> {
        let operation = self
            .0
            .as_ref()
            .map_err(|cause| super::failure::key_type(*cause))?;
        let result = operation
            .compare_retained_in_observed(left, right, allocator, &mut |count| observe(work, count))
            .map_err(|cause| match cause {
                ValueFailure::Caller(cause) => cause,
                ValueFailure::Value(ValueError::Allocator { source, .. }) => {
                    allocator.scalar_allocation_error("reserve map_agg key comparison", source)
                }
                ValueFailure::Value(cause) => KernelFailure::from_domain(cause),
            })?;
        match result {
            KeyComparison::True => Ok(true),
            KeyComparison::False => Ok(false),
            _ => Err(super::failure::contract(
                "map_agg key identity was not total",
            )),
        }
    }
}

fn observe(work: &mut ScalarWork<'_, '_>, count: usize) -> Result<(), KernelFailure> {
    for _ in 0..count {
        work.step()?;
    }
    Ok(())
}

impl<A: ScalarStateAllocator> RetainedKeyValue for TrackedAggScalarValue<A> {
    fn contents(&self, leaf: Option<KeyLeaf>) -> Result<RetainedKeyContents<'_>, ValueError> {
        use RetainedKeyContents as Value;
        Ok(match (self, leaf) {
            (Self::Bool(value), Some(KeyLeaf::Boolean)) => Value::Boolean(*value),
            (Self::Int64(value) | Self::Timestamp(value), Some(KeyLeaf::Signed)) => {
                Value::Signed(*value)
            }
            (Self::Date32(value), Some(KeyLeaf::Signed)) => Value::Signed(i64::from(*value)),
            (Self::Float64(value), Some(KeyLeaf::Float32)) => Value::Float32(*value as f32),
            (Self::Float64(value), Some(KeyLeaf::Float64)) => Value::Float64(*value),
            (Self::Decimal128(value), Some(KeyLeaf::Decimal128)) => Value::Decimal128(*value),
            (Self::Decimal256(value), Some(KeyLeaf::Decimal256)) => {
                Value::Decimal256(value.to_le_bytes())
            }
            (
                Self::Utf8(bytes) | Self::Binary(bytes),
                Some(KeyLeaf::Bytes | KeyLeaf::Character(_)),
            ) => Value::Bytes(bytes),
            (Self::Decimal128(value), Some(KeyLeaf::FixedBytes)) => {
                Value::Fixed16(value.to_be_bytes())
            }
            (Self::Temporal(original, value), Some(KeyLeaf::Temporal(expected)))
                if original == &expected =>
            {
                Value::Temporal(*value)
            }
            (Self::List(items), None) => Value::List(items.len()),
            (Self::Struct(fields), None) => Value::Struct(fields.len()),
            (Self::Map(entries), None) => Value::Map(entries.len()),
            _ => return Err(ValueError::invariant(ValueViolation::Carrier)),
        })
    }

    fn child(&self, kind: RetainedKeyChild, index: usize) -> Option<&dyn RetainedKeyValue> {
        let child = match (self, kind) {
            (Self::List(items), RetainedKeyChild::Item) => items.get(index)?,
            (Self::Struct(fields), RetainedKeyChild::Field) => fields.get(index)?,
            (Self::Map(entries), RetainedKeyChild::MapKey) => &entries.get(index)?.0,
            (Self::Map(entries), RetainedKeyChild::MapValue) => &entries.get(index)?.1,
            _ => return None,
        };
        child.as_ref().map(|value| value as &dyn RetainedKeyValue)
    }
}
